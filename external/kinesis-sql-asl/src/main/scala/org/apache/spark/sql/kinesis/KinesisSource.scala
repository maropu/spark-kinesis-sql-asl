/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kinesis

import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._


/**
 * A [[Source]] that uses the Kinesis Client Library to reads data from Amazon Kinesis.
 *
 * - The [[KinesisSourceOffset]] is the custom [[Offset]] defined for this source that contains
 *   a map of [[KinesisShard]] -> sequence number.
 *
 * - The [[KinesisSource]] written to do the following.
 *
 *   - When the source is created and `initialShardSeqNumbers` is initially evaluated, it launches
 *     [[StreamingContext]] and starts Kinesis receivers to store incoming stream blocks in
 *     executor-side [[org.apache.spark.storage.BlockManager]]s. The available stream blocks are
 *     reported via a callback function `collectKinesisStreamBlockData` and collected
 *     in `streamBlocksInBMs`.
 *
 *   - `getOffset()` returns the latest available offset of stream blocks in `streamBlocksInBMs`,
 *     which is returned as a [[KinesisSourceOffset]]. The latest offset consists of
 *     the latest sequence numbers of shards in `shardIdToLatestStoredSeqNumber` and the sequence
 *     numbers are updated every the function `collectKinesisStreamBlockData` call.
 *
 *   - `getBatch()` returns a DF including data between the offsets (`start offset`, `end offset`],
 *     i.e. `start offset` is exclusive because data in `start offset` have already
 *     been processed in a previous trigger.
 *
 *   - The DF returned is based on a [[KinesisSourceBlockRDD]] which consists of stream blocks
 *     stored in the executor-side [[org.apache.spark.storage.BlockManager]]s.
 */
private[kinesis] class KinesisSource(
    sqlContext: SQLContext,
    metadataPath: String,
    userSpecifiedSchema: Option[StructType],
    sourceOptions: Map[String, String])
  extends Source with Logging {

  import KinesisSource._
  import KinesisSourceOffset._

  /** A holder for stream blocks stored in workers */
  type StreamBlockInfo = (BlockId, SequenceNumberRanges, Boolean, Long)

  private val kinesisOptions = new KinesisOptions(sourceOptions)
  private val kinesisClient = {
    val cli = new AmazonKinesisClient(serializableAWSCredentials)
    cli.setEndpoint(kinesisOptions.endpointUrl)
    cli
  }

  private val _sparkSession = sqlContext.sparkSession
  private val _sc = sqlContext.sparkContext
  private val _ssc = new StreamingContext(_sc, Milliseconds(kinesisOptions.reportIntervalMs))

  /**
   * We internally use Spark Streaming logic to track latest sequence numbers of shards in streams
   * because Amazon Kinesis has no efficient API to track these numbers and tracking them
   * in a driver side causes excess load in batch processing.
   *
   * TODO: We need to reconsider this design.
   */
  private lazy val kinesisStreams = kinesisOptions.streamNames.flatMap { stream =>
    // Creates 1 Kinesis Receiver/input DStream for each shard
    val numStreams = kinesisClient.describeStream(stream).getStreamDescription.getShards.size
    logInfo(s"Create $numStreams streams for $stream")
    (0 until numStreams).map { i =>
      new KinesisSourceDStream(
        _ssc,
        stream,
        kinesisOptions.endpointUrl,
        kinesisOptions.regionId,
        kinesisOptions.initialPositionInStream,
        kinesisOptions.checkpointName,
        StorageLevel.MEMORY_AND_DISK,
        collectKinesisStreamBlockData,
        Some(serializableAWSCredentials)
      )
    }
  }

  /** This lock protects `shardIdToLatestStoredSeqNumber` and `streamBlocksInBMs`. */
  private val lockForUpdatingStreamBlockInfo = new ReentrantLock(true)

  /**
   * Latest sequence number ranges that have been stored successfully.
   */
  private val shardIdToLatestStoredSeqNumber = mutable.Map[KinesisShard, String]()

  /**
   * Sequence number ranges added to the current block being generated. Accessing and updating
   * of this map must be synchronized with `shardIdToLatestStoredSeqNumber`.
   */
  private val streamBlocksInBMs = mutable.Queue[StreamBlockInfo]()

  /**
   * A holder of the offset until which processes have been done.
   * Entries from an earlier offset to `currentOffset` in `streamBlocksInBMs` are safely
   * removed in the purge thread.
   */
  private var currentOffset: Option[Offset] = None

  private lazy val initialShardSeqNumbers = {
    val metadataLog = new HDFSMetadataLog[KinesisSourceOffset](_sparkSession, metadataPath)
    val seqNumbers = metadataLog.get(0).getOrElse {
      val offsets = KinesisSourceOffset(fetchAllShardEarliestSeqNumber())
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.shardToSeqNum

    seqNumbers.map { case (shard, seqNumber) =>
      shardIdToLatestStoredSeqNumber += shard -> seqNumber
    }

    // Initialize a current offset with earliest one
    currentOffset = Some(KinesisSourceOffset(seqNumbers))

    // Launch a spark streaming context
    _ssc.union(kinesisStreams).foreachRDD(_ => {})
    _ssc.start()

    seqNumbers
  }

  private val dataFormat = KinesisDataFormatFactory.create(kinesisOptions.format)

  private lazy val _schema = userSpecifiedSchema.getOrElse {
    KinesisSource.inferSchema(sqlContext, sourceOptions)
  }

  override val schema: StructType = _schema

  private val schemaWithoutTimestamp = StructType(schema.drop(1))

  private val invalidStreamBlockId = StreamBlockId(0, 0L)
  private val minimumSeqNumber = ""

  private def synchronizeStreamBlocks[T](func: => T): Option[T] = {
    var retValue: Option[T] = None
    lockForUpdatingStreamBlockInfo.lock()
    try {
      retValue = Option(func)
    } catch {
      case e: Throwable =>
        reportDataLoss("May lose data because of an exception occurred "
          + "when updating stream blocks information: " + e)
    } finally {
      lockForUpdatingStreamBlockInfo.unlock()
    }
    retValue
  }

  /**
   * Callback method called after a block has been generated.
   */
  private def collectKinesisStreamBlockData(
      blockIds: Array[BlockId],
      arrayOfSeqNumberRanges: Array[SequenceNumberRanges],
      isBlockIdValid: Array[Boolean],
      arrayOfNumRecords: Array[Long]): Unit = {
    if (blockIds.length == 0) return

    logDebug(s"Received kinesis stream blocked data: ${arrayOfSeqNumberRanges.mkString(", ")}")

    // Pre-process for updating stream block information
    val newBlocks = Seq.tabulate(blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      (blockIds(i), arrayOfSeqNumberRanges(i), isValid, arrayOfNumRecords(i))
    }
    val newLatestSeqNumbers = newBlocks.flatMap { case (_, SequenceNumberRanges(ranges), _, _) =>
      ranges.map { case range =>
        val shard = KinesisShard(range.streamName, range.shardId)
        (shard, range.toSeqNumber)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).max)

    // Append pending blocks and update the latest sequence numbers of shards
    synchronizeStreamBlocks {
      streamBlocksInBMs ++= newBlocks
      shardIdToLatestStoredSeqNumber ++= newLatestSeqNumbers
      logDebug(s"Pending stream blocks are: ${streamBlocksInBMs.map(_._2).mkString(", ")}")
      logDebug("shardIdToLatestStoredSeqNumber:"
        + shardIdToLatestStoredSeqNumber.map { case (shard, latestSeqNumber) =>
          s"(${shard.streamName}, ${shard.shardId}, $latestSeqNumber)"
        }.mkString(", "))
    }
  }

  override def getOffset: Option[Offset] = {
    // Make sure that this source is initialized
    initialShardSeqNumbers

    synchronizeStreamBlocks {
      val offset = kinesisOptions.softLimitMaxRecordsPerTrigger match {
        case limit if limit > 0 =>
          // Control processing number of records per trigger to prevent Spark from
          // invoking many tasks in a job.
          limitBatch(limit)
        case _ =>
          KinesisSourceOffset(shardIdToLatestStoredSeqNumber.clone().toMap)
      }
      logDebug(s"getOffset: ${offset.shardToSeqNum.toSeq.sorted(kinesisOffsetOrdering)}")
      offset
    }
  }

  /** Limit number of records fetched from shards. */
  private def limitBatch(maxRecords: Int): KinesisSourceOffset = {
    currentOffset.map { offset =>
      val kinesisCurrentOffset = KinesisSourceOffset.getShardSeqNumbers(offset)
      val (limitedOffset, numRecords) = streamBlocksInBMs
        .foldLeft((mutable.Map(kinesisCurrentOffset.toSeq: _*), 0L)) {
            case ((offset, sumRecords), (_, SequenceNumberRanges(ranges), _, n)) =>

          // Apply the soft limit here
          if (sumRecords < maxRecords) {
            ranges.foreach { range =>
              offset.put(KinesisShard(range.streamName, range.shardId), range.toSeqNumber)
            }
            (offset, sumRecords + n)
          } else {
            (offset, sumRecords)
          }
      }
      logDebug(s"Select $numRecords records from ${streamBlocksInBMs.map(_._4).sum} records " +
        "to limit batch size per trigger")
      KinesisSourceOffset(limitedOffset.toMap)
    }.getOrElse {
      sys.error("currentOffset not initialized")
    }
  }

  /**
   * Returns the data that is between the offsets
   * (`start.get.shardToSeqNum`, `end.shardToSeqNum`], i.e. `start.get.shardToSeqNum` is exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure that this source is initialized
    initialShardSeqNumbers

    logInfo(s"getBatch called with start = $start, end = $end")
    val untilShardSeqNumbers = KinesisSourceOffset.getShardSeqNumbers(end)
    val fromShardSeqNumbers = start match {
      case Some(prevBatchEndOffset) =>
        KinesisSourceOffset.getShardSeqNumbers(prevBatchEndOffset)
      case None =>
        initialShardSeqNumbers
    }

    // Find the new shards in streams, and get their earliest sequence numbers
    val newShards = untilShardSeqNumbers.keySet.diff(fromShardSeqNumbers.keySet)
    val newShardSeqNumbers = if (newShards.nonEmpty) {
      val newShardsMap = newShards.map(s => (s, fetchShardEarliestSeqNumber(s))).toMap
      if (newShardsMap.keySet != newShards) {
        // We cannot get from sequence numbers for some shards. It means they got deleted.
        val deletedShards = newShards.diff(newShardsMap.keySet)
        reportDataLoss(s"Cannot find earliest sequence numbers of ${deletedShards}. " +
          "Some data may have been missed")
      }
      logInfo(s"Shards added: $newShardsMap")
      newShardsMap
    } else {
      Map.empty[KinesisShard, String]
    }

    val deletedShards = fromShardSeqNumbers.keySet.diff(untilShardSeqNumbers.keySet)
    if (deletedShards.nonEmpty) {
      reportDataLoss(s"$deletedShards are gone. Some data may have been missed")
    }

    // Calculate sequence ranges
    val targetRanges = (newShardSeqNumbers ++ fromShardSeqNumbers).map { case (shard, from) =>
      SequenceNumberRange(
        shard.streamName,
        shard.shardId,
        from,
        // This unexpected behaviour is handled in a next range check
        untilShardSeqNumbers.asJava.getOrDefault(shard, minimumSeqNumber))
    }.filter { range =>
      if (range.toSeqNumber < range.fromSeqNumber) {
        reportDataLoss(s"Shard ${range.shardId}'s offset in ${range.streamName} was changed " +
          s"from ${range.fromSeqNumber} to ${range.toSeqNumber}, some data may have been missed")
        false
      } else if (range.fromSeqNumber == range.toSeqNumber) {
        // Remove the shards that have no new stream data
        false
      } else {
        true
      }
    }.toSeq

    // Helper function to check if ranges are qualified
    def checkIfQualified(block: StreamBlockInfo): Boolean = block match {
      case (_, SequenceNumberRanges(ranges), _, _) =>
        ranges.forall { range =>
          targetRanges.exists { targetRange =>
            if (range.streamName == targetRange.streamName &&
                range.shardId == targetRange.shardId) {
              if (range.fromSeqNumber <= targetRange.fromSeqNumber) {
                reportDataLoss(s"Illegal stream block detected: ${block._2}")
              }
              range.toSeqNumber <= targetRange.toSeqNumber
            } else {
              false
            }
          }
        }
    }

    val targetBlocks = synchronizeStreamBlocks {
      val candidiateBlocks = mutable.ArrayBuffer[StreamBlockInfo]()
      while (streamBlocksInBMs.nonEmpty && checkIfQualified(streamBlocksInBMs.head)) {
        candidiateBlocks += streamBlocksInBMs.dequeue()
      }

      // If we cannot find any qualified stream block in `streamBlocksInBMs`,
      // we fetch all the data via Kinesis iterators.
      if (candidiateBlocks.nonEmpty) {
        candidiateBlocks.map(d => (d._1, d._2, d._3)).unzip3
      } else {
        null
      }
    }.getOrElse {
      Seq.tabulate(targetRanges.length) { i =>
        (invalidStreamBlockId, SequenceNumberRanges(targetRanges(i)), false)
      }.unzip3
    }

    // Update the offset where processes have been done
    currentOffset = Some(end)

    // Create a RDD that reads from Amazon Kinesis and get inputs as binary data
    val baseRdd = new KinesisSourceBlockRDD(
      _sc,
      kinesisOptions.regionId,
      kinesisOptions.endpointUrl,
      _blockIds = targetBlocks._1.toArray,
      arrayOfseqNumberRanges = targetBlocks._2.toArray,
      isBlockIdValid = targetBlocks._3.toArray,
      retryTimeoutMs = kinesisOptions.retryTimeoutMs,
      awsCredentialsOption = Option(serializableAWSCredentials)
    )
    val serializableReadFunc = dataFormat.buildReader(
      _sparkSession, schemaWithoutTimestamp, sourceOptions)
    val rdd = baseRdd.mapPartitionsInternal { iter =>
      val dataOnMem = iter.toSeq
      val timestampRows = dataOnMem.map(_._1).map(InternalRow(_))
      val valueRows = serializableReadFunc(dataOnMem.map(_._2).toIterator)
      timestampRows.toIterator.zip(valueRows).map{ row =>
        new JoinedRow(row._1, row._2)
      }.toIterator.asInstanceOf[Iterator[InternalRow]]
    }
    val logicalRdd = LogicalRDD(schema.toAttributes, rdd)(_sparkSession)
    Dataset.ofRows(_sparkSession, logicalRdd)
  }

  // Fetches the earliest offsets for all the shards in given streams
  private def fetchAllShardEarliestSeqNumber(): Map[KinesisShard, String] = {
    val allShards = kinesisOptions.streamNames.map { streamName =>
      val shards = kinesisClient.describeStream(streamName).getStreamDescription.getShards
      logDebug(s"Number of assigned to $streamName: ${shards.size}")
      (streamName, shards)
    }
    allShards.flatMap { case (streamName, shards) =>
      val shardSeqNumbers = shards.asScala.map { case shard =>
        (KinesisShard(streamName, shard.getShardId),
          shard.getSequenceNumberRange.getStartingSequenceNumber)
      }
      logDebug(s"Got sequence numbers for $streamName: $shardSeqNumbers")
      shardSeqNumbers
    }.toMap
  }

  // Fetches the earliest offsets for shards
  private def fetchShardEarliestSeqNumber(shard: KinesisShard): String = {
    val shards = kinesisClient.describeStream(shard.streamName).getStreamDescription.getShards
    shards.asScala.find(_.getParentShardId == shard.shardId)
      .map(_.getSequenceNumberRange.getStartingSequenceNumber)
      .getOrElse {
        logWarning(s"Unknown shard detected: ${shard.shardId}")
        minimumSeqNumber
      }
  }

  override def stop(): Unit = {
    _ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (kinesisOptions.failOnDataLoss) {
      throw new IllegalStateException(message +
        ". Set the source option 'failOnDataLoss' to 'false' if you want to ignore these checks.")
    } else {
      logWarning(message)
    }
  }
}

private[kinesis] object KinesisSource {

  val serializableAWSCredentials = {
    val awsCredentials = new DefaultAWSCredentialsProviderChain().getCredentials
    SerializableAWSCredentials(
      awsCredentials.getAWSAccessKeyId,
      awsCredentials.getAWSSecretKey
    )
  }

  def withTimestamp(schema: StructType): StructType = {
    StructType(StructField("timestamp", TimestampType, nullable = false) +: schema)
  }

  def inferSchema(
      sqlContext: SQLContext,
      sourceOptions: Map[String, String]): StructType = {
    val kinesisOptions = new KinesisOptions(sourceOptions)
    val dataFormat = KinesisDataFormatFactory.create(kinesisOptions.format)
    val baseRdd = new KinesisSnapshotRDD(
      sqlContext.sparkContext,
      kinesisOptions.regionId,
      kinesisOptions.endpointUrl,
      kinesisOptions.streamNames,
      limitMaxRecordsToInferSchema = kinesisOptions.limitMaxRecordsToInferSchema,
      retryTimeoutMs = kinesisOptions.retryTimeoutMs,
      awsCredentialsOption = Some(serializableAWSCredentials)
    )
    if (baseRdd.isEmpty()) {
      throw new IllegalStateException("No stream data exists for inferring a schema")
    }
    val valueSchema = dataFormat.inferSchema(sqlContext.sparkSession, baseRdd, sourceOptions)
    withTimestamp(valueSchema)
  }
}
