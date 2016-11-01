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

package org.apache.spark.examples.streaming

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random
import scala.collection.JavaConversions._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.apache.commons.io.FileUtils

object KinesisStreamWriter {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        """
          |Usage: KinesisStreamWriter <stream-name> <endpoint-url> <records-per-sec> <filename>
          |
          |  <stream-name>     : the name of the Kinesis stream
          |  <endpoint-url>    : the endpoint of the Kinesis service
          |  <num-threads>     : the number of threads
          |  <records-per-sec> : the rate of records/s for each thread to put onto the stream
          |  <filename>        : the filename of records to put onto the stream
        """.stripMargin)

      System.exit(1)
    }

    // Populate the appropriate variables from the given args
    val Array(stream, endpoint, numThreads, recordsPerSecond, fileName) = args

    require(numThreads.toInt > 0, s"Number of threads must be positive: $numThreads")
    require(new File(fileName).exists(), s"$fileName does not exist")

    println(s"$numThreads threads put records onto stream $stream and endpoint $endpoint " +
      s"at a rate of $recordsPerSecond records per second")

    val records = FileUtils.readLines(new File(fileName)).toSeq
    val maxRecordNum = records.size

    for (i <- 0 until numThreads.toInt) {
      new Thread(new Runnable() {

        private val producer: KinesisProducer = {
          val conf = new KinesisProducerConfiguration()
            .setRecordMaxBufferedTime(1000)
            .setMaxConnections(1)
            .setRegion(RegionUtils.getRegionMetadata.getRegionByEndpoint(endpoint).getName)
            .setMetricsLevel("summary")

          new KinesisProducer(conf)
        }

        override def run(): Unit = {
          // Create the low-level Kinesis Client from the AWS Java SDK
          val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
          kinesisClient.setEndpoint(endpoint)

          val recordIterator = new Iterator[String] {
            val rnd = new Random(i)

            override def hasNext: Boolean = true

            override def next(): String =
              records.get(rnd.nextInt(maxRecordNum))
          }

          while (true) {
            // Count # of success and failure for a producer
            val (numSuccess, numFailure) = (new AtomicInteger(0), new AtomicInteger(0))

            // Generate recordsPerSec records to put onto the stream via KPL
            val rows = (1 to recordsPerSecond.toInt).map { recordNum =>
              val str = recordIterator.next()
              val data = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
              val partitionKey = s"partitionKey-$recordNum"

              val future = producer.addUserRecord(stream, partitionKey, data)

              val kinesisCallBack = new FutureCallback[UserRecordResult]() {

                override def onSuccess(result: UserRecordResult): Unit = {
                  numSuccess.incrementAndGet()
                }

                override def onFailure(t: Throwable): Unit = {
                  numFailure.incrementAndGet()
                }
              }
              Futures.addCallback(future, kinesisCallBack)
            }
            producer.flushSync()

            println(s"Thread-$i succeeded to send $numSuccess records and " +
              s"failed to send $numFailure records")

            // Sleep for a second
            Thread.sleep(1000)
          }
        }
      }).start()
    }
  }
}
