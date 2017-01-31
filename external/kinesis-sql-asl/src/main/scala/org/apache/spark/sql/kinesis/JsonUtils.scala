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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Utilities for converting kinesis related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  type KinesisOffset = (KinesisShard, String)

  /**
   * Read per-KinesisShard offsets from json string
   */
  def partitionOffsets(str: String): Map[KinesisShard, String] = {
    try {
      Serialization.read[Map[String, Map[String, String]]](str).flatMap { case (streamName, offsets) =>
        offsets.map { case (shardId, offset) =>
          KinesisShard(streamName, shardId) -> offset
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
   * Write per-KinesisShard offsets as json string
   */
  def partitionOffsets(shardToSeqNum: Map[KinesisShard, String]): String = {
    val result = new HashMap[String, HashMap[String, String]]()

    implicit val ordering = new Ordering[KinesisOffset] {
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
    val shards = shardToSeqNum.toSeq.sorted  // sort for more determinism
    shards.foreach { case (shard, off) =>
      val parts = result.getOrElse(shard.streamName, new HashMap[String, String])
      parts += shard.shardId -> off
      result += shard.streamName -> parts
    }
    Serialization.write(result)
  }
}
