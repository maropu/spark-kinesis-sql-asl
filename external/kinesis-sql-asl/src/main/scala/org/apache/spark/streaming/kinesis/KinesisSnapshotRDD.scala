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

import java.util.Date

import scala.collection.JavaConverters._

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

private[kinesis] case class KinesisSnapshotPartition(
  override val index: Int,
  streamName: String,
  shardId: String,
  fromSeqNumber: String) extends Partition

/**
 * A RDD holding stream data at from the earliest to current time.
 * This is used for inferring the schema of stream data.
 */
private[spark] class KinesisSnapshotRDD(
    sc: SparkContext,
    regionName: String,
    endpointUrl: String,
    streams: Seq[String],
    recordLimitForPartition: Int = 10000,
    retryTimeoutMs: Int = 10000,
    messageHandler: Record => Array[Byte] = KinesisSnapshotRDD.msgHandler,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None,
    private val currentTime: Date = new Date(System.currentTimeMillis())
  ) extends RDD[Array[Byte]](sc, Nil) with Logging {

  @transient private val client = {
    val credentials = awsCredentialsOption.getOrElse {
      new DefaultAWSCredentialsProviderChain().getCredentials
    }
    val cli = new AmazonKinesisClient(credentials)
    cli.setEndpoint(endpointUrl)
    cli
  }

  override def getPartitions: Array[Partition] = {
    streams.flatMap { stream =>
      val shards = client.describeStream(stream).getStreamDescription.getShards
      shards.asScala.map { shard =>
        val fromSeqNumber = shard.getSequenceNumberRange.getStartingSequenceNumber
        (stream, shard.getShardId, fromSeqNumber)
      }.zipWithIndex.map { case ((streamName, shard, seqNumber), index) =>
        KinesisSnapshotPartition(index, streamName, shard, seqNumber)
      }
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val partition = split.asInstanceOf[KinesisSnapshotPartition]
    val credentials = awsCredentialsOption.getOrElse {
      new DefaultAWSCredentialsProviderChain().getCredentials
    }
    val range = SequenceNumberRange(
      partition.streamName, partition.shardId, partition.fromSeqNumber, null)
    new KinesisSnapshotIterator(
      credentials, endpointUrl, regionName, range, currentTime, recordLimitForPartition,
      retryTimeoutMs).map(messageHandler)
  }
}

private object KinesisSnapshotRDD {

  def msgHandler: (Record => Array[Byte]) = new (Record => Array[Byte]) with Serializable {

    override def apply(record: Record): Array[Byte] = {
      record.getData.array
    }
  }
}

private class KinesisSnapshotIterator(
    credentials: AWSCredentials,
    endpointUrl: String,
    regionId: String,
    range: SequenceNumberRange,
    currentTime: Date,
    recordLimitForPartition: Int,
    retryTimeoutMs: Int)
  extends KinesisSequenceRangeIterator(
    credentials,
    endpointUrl,
    regionId,
    range,
    retryTimeoutMs) with Logging {

  var _nextValue: Record = getNextInternal()
  var _numReadRecord: Int = 0

  private def getNextInternal(): Record = {
    try {
      val record = super.getNext()
      val recordTime = record.getApproximateArrivalTimestamp
      if (currentTime.compareTo(recordTime) < 0 || _numReadRecord >= recordLimitForPartition) {
        finished = true
      }
      _numReadRecord = _numReadRecord + 1
      record
    } catch {
      case _: Throwable => null
    }
  }

  override protected def getNext(): Record = {
    val curValue = _nextValue
    _nextValue = getNextInternal()
    curValue
  }
}
