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

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.model._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.kinesis.KinesisSourceDStream.KinesisSourceType

/**
 * A BlockRDD where the block data is backed by Kinesis, which can accessed using the
 * sequence numbers of the corresponding blocks.
 */
private[spark] class KinesisSourceBlockRDD(
    sc: SparkContext,
    override val regionName: String,
    override val endpointUrl: String,
    @transient override val _blockIds: Array[BlockId],
    @transient override val arrayOfseqNumberRanges: Array[SequenceNumberRanges],
    @transient override val isBlockIdValid: Array[Boolean] = Array.empty,
    override val retryTimeoutMs: Int = 10000,
    override val awsCredentialsOption: Option[SerializableAWSCredentials] = None
  ) extends KinesisBackedBlockRDD[KinesisSourceType](
    sc,
    regionName,
    endpointUrl,
    _blockIds,
    arrayOfseqNumberRanges,
    isBlockIdValid,
    retryTimeoutMs,
    KinesisSourceDStream.msgHandler,
    awsCredentialsOption) {

  override def compute(split: Partition, context: TaskContext): Iterator[KinesisSourceType] = {
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[KinesisBackedBlockRDDPartition]
    val blockId = partition.blockId

    def getBlockFromBlockManager(): Option[Iterator[KinesisSourceType]] = {
      logDebug(s"Read partition data of $this from block manager, block $blockId")
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[KinesisSourceType]])
    }

    def getBlockFromKinesis(): Iterator[KinesisSourceType] = {
      val credentials = awsCredentialsOption.getOrElse {
        new DefaultAWSCredentialsProviderChain().getCredentials()
      }
      partition.seqNumberRanges.ranges.iterator.flatMap { range =>
        new KinesisSourceRangeIterator(credentials, endpointUrl, regionName, range, retryTimeoutMs)
          .map(messageHandler)
      }
    }
    if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromKinesis() }
    } else {
      getBlockFromKinesis()
    }
  }
}

/**
 * Returns the records that range between the offsets
 * (`range.fromSeqNumber`, `range.toSeqnumber`], i.e. `range.fromSeqNumber` is exclusive.
 */
private class KinesisSourceRangeIterator(
    credentials: AWSCredentials,
    endpointUrl: String,
    regionId: String,
    range: SequenceNumberRange,
    retryTimeoutMs: Int)
  extends KinesisSequenceRangeIterator(
    credentials,
    endpointUrl,
    regionId,
    range,
    retryTimeoutMs) with Logging {

  override protected def getNext(): Record = {
    val curRecord = super.getNext()
    if (finished || curRecord.getSequenceNumber != range.fromSeqNumber) {
      curRecord
    } else {
      // Skip a record if it has `range.fromSeqNumber`
      this.getNext()
    }
  }
}
