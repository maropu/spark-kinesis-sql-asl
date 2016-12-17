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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream}

object KinesisProducerHolder {

  private var producer: Option[KinesisProducer] = None

  private def createProducer(endpoint: String, options: Map[String, String]): KinesisProducer = {
    val conf = new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(options.getOrElse("recordMaxBufferedTime", "1000").toInt)
      .setMaxConnections(options.getOrElse("maxConnections", "1").toInt)
      .setRegion(RegionUtils.getRegionMetadata.getRegionByEndpoint(endpoint).getName)
      .setMetricsLevel("summary")
    new KinesisProducer(conf)
  }

  def get(endpoint: String, options: Map[String, String]): KinesisProducer = {
    producer.getOrElse {
      producer = Some(createProducer(endpoint, options))
      producer.get
    }
  }
}

final class KinesisDStreamFunctions[T: ClassTag](@transient self: DStream[T])
    extends Serializable with Logging {

  def saveAsKinesisStream(
      stream: String, endpoint: String, options: Map[String, String] = Map.empty)
    : Unit = self.ssc.withScope {
    val saveFunc = (rdd: RDD[T], time: Time) => {
      rdd.foreachPartition { iter =>
        val producer = KinesisProducerHolder.get(endpoint, options)
        iter.zipWithIndex.foreach { case (data, index) =>
          val blob = ByteBuffer.wrap(s"$data".getBytes(StandardCharsets.UTF_8))
          val partitionKey = s"partitionKey-$index"
          val future = producer.addUserRecord(stream, partitionKey, blob)
          val kinesisCallBack = new FutureCallback[UserRecordResult]() {
            override def onSuccess(result: UserRecordResult): Unit = {}
            override def onFailure(t: Throwable): Unit = {}
          }
          Futures.addCallback(future, kinesisCallBack)
        }
        producer.flushSync()
      }
    }
    new ForEachDStream(self, self.context.sparkContext.clean(saveFunc, false), false).register()
  }
}

object KinesisDStreamFunctions {

  /**
   * Implicitly inject the [[KinesisDStreamFunctions]] into [[DStream]].
   */
  implicit def dstreamToKinesisStreamFunctions[T: ClassTag](self: DStream[T])
    : KinesisDStreamFunctions[T] = {
    new KinesisDStreamFunctions[T](self)
  }
}
