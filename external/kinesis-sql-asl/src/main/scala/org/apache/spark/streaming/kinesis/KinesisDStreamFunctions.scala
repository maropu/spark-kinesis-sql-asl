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

import scala.reflect.ClassTag

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream}

object KinesisProducerHolder extends Logging {

  private var producer: Option[KinesisProducer] = None

  private def resolveAWSCredentialsProvider(
    awsCredentialsOption: Option[SerializableAWSCredentials])
    : AWSCredentialsProvider = awsCredentialsOption match {
    case Some(awsCredentials) =>
      logInfo("Using provided AWS credentials")
      new AWSCredentialsProvider {
        override def getCredentials: AWSCredentials = awsCredentials
        override def refresh(): Unit = {}
      }
    case None =>
      logInfo("Using DefaultAWSCredentialsProviderChain")
      new DefaultAWSCredentialsProviderChain()
  }

  private def createProducer(
    endpoint: String,
    awsCredentialsOption: Option[SerializableAWSCredentials],
    otherOptions: Map[String, String])
    : KinesisProducer = {
    val conf = new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(otherOptions.getOrElse("recordMaxBufferedTime", "1000").toInt)
      .setMaxConnections(otherOptions.getOrElse("maxConnections", "1").toInt)
      .setRegion(RegionUtils.getRegionMetadata.getRegionByEndpoint(endpoint).getName)
      .setMetricsLevel("summary")
      .setCredentialsProvider(resolveAWSCredentialsProvider(awsCredentialsOption))
    new KinesisProducer(conf)
  }

  def get(
    endpoint: String,
    awsCredentialsOption: Option[SerializableAWSCredentials],
    options: Map[String, String])
    : KinesisProducer = {
    producer.getOrElse {
      producer = Some(createProducer(endpoint, awsCredentialsOption, options))
      producer.get
    }
  }
}

final class KinesisDStreamFunctions[T: ClassTag](@transient self: DStream[T])
    extends Serializable with Logging {

  private def defaultPartitioner(data: T, index: Int): String = {
    s"partitionKey-${index}"
  }

  private def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      partitioner: (T, Int) => String,
      serializableAWSCredentials: Option[SerializableAWSCredentials],
      otherOptions: Map[String, String] = Map.empty)
    : Unit = self.ssc.withScope {
    val saveFunc = (rdd: RDD[T], time: Time) => {
      rdd.foreachPartition { iter =>
        val producer = KinesisProducerHolder.get(endpoint, serializableAWSCredentials, otherOptions)
        iter.zipWithIndex.foreach { case (data, index) =>
          val blob = ByteBuffer.wrap(msgHandler(data))
          val future = producer.addUserRecord(stream, partitioner(data, index), blob)
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

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      otherOptions: Map[String, String] = Map.empty): Unit = {
    saveAsKinesisStream(stream, endpoint, msgHandler, defaultPartitioner _, None, otherOptions)
  }

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      partitioner: (T, Int) => String,
      otherOptions: Map[String, String] = Map.empty): Unit = {
    saveAsKinesisStream(stream, endpoint, msgHandler, partitioner, None, otherOptions)
  }

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      awsAccessKeyId: String,
      awsSecretKey: String,
      otherOptions: Map[String, String] = Map.empty): Unit = {
    saveAsKinesisStream(
      stream,
      endpoint,
      msgHandler,
      defaultPartitioner _,
      Some(SerializableAWSCredentials(awsAccessKeyId, awsSecretKey)),
      otherOptions)
  }

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      partitioner: (T, Int) => String,
      awsAccessKeyId: String,
      awsSecretKey: String,
      otherOptions: Map[String, String] = Map.empty): Unit = {
    saveAsKinesisStream(
      stream,
      endpoint,
      msgHandler,
      partitioner,
      Some(SerializableAWSCredentials(awsAccessKeyId, awsSecretKey)),
      otherOptions)
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
