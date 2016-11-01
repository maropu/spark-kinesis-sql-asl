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

package org.apache.spark.streaming.kinesis

import java.sql.Timestamp

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kinesis.KinesisSourceDStream.KinesisSourceType
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.{Duration, StreamingContext}

private[spark] class KinesisSourceDStream(
    _ssc: StreamingContext,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel,
    @transient callbackFunc: (Array[BlockId], Array[SequenceNumberRanges], Array[Boolean]) => Unit,
    awsCredentialsOption: Option[SerializableAWSCredentials]
  ) extends KinesisInputDStream[KinesisSourceType](
    _ssc,
    streamName,
    endpointUrl,
    regionName,
    initialPositionInStream,
    checkpointAppName,
    checkpointInterval,
    storageLevel,
    KinesisSourceDStream.msgHandler,
    awsCredentialsOption) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo])
    : RDD[KinesisSourceType] = {
    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges = blockInfos
        .map { _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
      logDebug(s"Creating KinesisBackedBlockRDD for $time with ${seqNumRanges.length} " +
          s"seq number ranges: ${seqNumRanges.mkString(", ")} ")
      callbackFunc(blockIds, seqNumRanges, isBlockIdValid)
      // Returns empty RDD because `KinesisSource` does not use the output
      _ssc.sparkContext.emptyRDD[KinesisSourceType]
    } else {
      logWarning("Kinesis sequence number information was not present with some block metadata," +
        " it may not be possible to recover from failures")
      super.createBlockRDD(time, blockInfos)
    }
  }

  override def getReceiver(): Receiver[KinesisSourceType] = {
    new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream,
      checkpointAppName, checkpointInterval, storageLevel, KinesisSourceDStream.msgHandler,
      awsCredentialsOption,
      /**
       * Disables automatically running Kinesis chekpoints in receivers because Spark does
       * checkpoints in [[org.apache.spark.sql.execution.streaming.MetadataLog]].
       *
       * TODO: How to invoke Kinesis checkpoints in streams by Spark.
       */
      true)
  }
}

private[spark] object KinesisSourceDStream {

  type KinesisSourceType = (Long, Array[Byte])

  def msgHandler = new (Record => KinesisSourceType) with Serializable {

    override def apply(record: Record): KinesisSourceType = {
      val timestamp = DateTimeUtils.fromJavaTimestamp(
        new Timestamp(record.getApproximateArrivalTimestamp.getTime))
      (timestamp, record.getData.array)
    }
  }
}
