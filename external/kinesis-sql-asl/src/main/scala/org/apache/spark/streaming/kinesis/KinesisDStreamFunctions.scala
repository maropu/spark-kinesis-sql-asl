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
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream}


/** An option set for the Kinesis Producer Library. */
private[kinesis] class KinesisOptions(parameters: Map[String, String])
    extends Logging with Serializable {

  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toInt
      } catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }

  val endpoint = parameters.getOrElse("endpoint",
    throw new IllegalArgumentException("`endpoint` not defined"))

  val aggregationMaxCount = getInt("aggregationMaxCount", Int.MaxValue)
  val aggregationMaxSize = getInt("aggregationMaxSize", 51200)
  val collectionMaxCount = getInt("collectionMaxCount", 500)
  val collectionMaxSize = getInt("collectionMaxSize", 5242880)
  val rateLimit = getInt("rateLimit", 150)
  val recordMaxBufferedTime = getInt("recordMaxBufferedTime", 100)
  val maxConnections = getInt("maxConnections", 24)
  val metricsLevel = parameters.getOrElse("metricsLevel", "detailed")
}

private[kinesis] object KinesisProducerFactory extends Logging {

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

  // TODO: Do we need to cache `KinesisProducer`?
  def get(awsCredentialsOption: Option[SerializableAWSCredentials], options: KinesisOptions)
    : KinesisProducer = {
    val conf = new KinesisProducerConfiguration()
      .setCredentialsProvider(resolveAWSCredentialsProvider(awsCredentialsOption))
      .setRegion(RegionUtils.getRegionMetadata.getRegionByEndpoint(options.endpoint).getName)
      .setAggregationMaxCount(options.aggregationMaxCount)
      .setAggregationMaxSize(options.aggregationMaxSize)
      .setCollectionMaxCount(options.collectionMaxCount)
      .setCollectionMaxSize(options.collectionMaxSize)
      .setRateLimit(options.rateLimit)
      .setRecordMaxBufferedTime(options.recordMaxBufferedTime)
      .setMaxConnections(options.maxConnections)
      .setMetricsLevel(options.metricsLevel)
    new KinesisProducer(conf)
  }
}

/**
 * Extra functions available on DStream for Amazon Kinesis through an implicit conversion.
 */
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
      otherOptions: Map[String, String])
    : Unit = self.ssc.withScope {
    val kinesisOptions = new KinesisOptions(otherOptions + ("endpoint" -> endpoint))
    val saveFunc = (rdd: RDD[T], time: Time) => {
      rdd.foreachPartition { iter =>
        val producer = KinesisProducerFactory.get(serializableAWSCredentials, kinesisOptions)
        iter.zipWithIndex.foreach { case (data, index) =>
          val blob = ByteBuffer.wrap(msgHandler(data))
          val future = producer.addUserRecord(stream, partitioner(data, index), blob)
          val kinesisCallBack = new FutureCallback[UserRecordResult]() {
            override def onSuccess(result: UserRecordResult): Unit = {}
            override def onFailure(t: Throwable): Unit = {
              logError("Failed to send data:" + data)
            }
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
      msgHandler: T => Array[Byte]): Unit = {
    saveAsKinesisStream(stream, endpoint, msgHandler, Map.empty)
  }

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      otherOptions: Map[String, String]): Unit = {
    saveAsKinesisStream(stream, endpoint, msgHandler, defaultPartitioner _, None, otherOptions)
  }

  def saveAsKinesisStream(
      stream: String,
      endpoint: String,
      msgHandler: T => Array[Byte],
      awsAccessKeyId: String,
      awsSecretKey: String,
      otherOptions: Map[String, String]): Unit = {
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
      awsAccessKeyId: String,
      awsSecretKey: String,
      partitioner: (T, Int) => String,
      otherOptions: Map[String, String]): Unit = {
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
