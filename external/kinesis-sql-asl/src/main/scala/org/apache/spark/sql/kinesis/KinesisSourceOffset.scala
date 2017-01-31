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

import org.apache.spark.sql.execution.streaming.Offset

/** A holder for shard ids in streams */
private[spark] case class KinesisShard(streamName: String, shardId: String)

/**
 * An [[Offset]] for the [[org.apache.spark.sql.kinesis.KinesisSource]]. This one tracks
 * all partitions of shards and their sequence numbers.
 */
private[kinesis] case class KinesisSourceOffset(
    shardToSeqNum: Map[KinesisShard, String]) extends Offset {
  import KinesisSourceOffset._

  override def toString(): String = {
    shardToSeqNum.toSeq.sorted(kinesisOffsetOrdering).mkString("[", ", ", "]")
  }
}

private[kinesis] object KinesisSourceOffset {

  type KinesisOffset = (KinesisShard, String)

  val kinesisOffsetOrdering = new Ordering[KinesisOffset] {

    override def compare(x: KinesisOffset, y: KinesisOffset): Int = {
      if (x._1.streamName != y._1.streamName) {
        x._1.streamName.compare(y._1.streamName)
      } else {
        if (x._1.shardId != y._1.shardId) {
          x._1.shardId.compare(y._1.shardId)
        } else {
          x._2.compare(y._2)
        }
      }
    }
  }

  /**
   * Returns [[KinesisSourceOffset]] from a variable sequence of (streamName, shardId, seqNum)
   * tuples.
   */
  def apply(offsetTuples: (String, String, String)*): KinesisSourceOffset = {
    KinesisSourceOffset(offsetTuples.map { case (streamName, shardId, seqNum) =>
        (KinesisShard(streamName, shardId), seqNum)
      }.toMap
    )
  }

  def getShardSeqNumbers(offset: Offset): Map[KinesisShard, String] = {
    offset match {
      case o: KinesisSourceOffset => o.shardToSeqNum
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KinesisSourceOffset")
    }
  }
}
