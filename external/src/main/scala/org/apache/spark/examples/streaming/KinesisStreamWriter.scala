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

import scala.util.Random
import scala.collection.JavaConversions._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
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
            // Generate recordsPerSec records to put onto the stream
            val rows = (1 to recordsPerSecond.toInt).map { recordNum =>
              // Create a PutRecordRequest with an Array[Byte] version of the data
              val putRecordRequest = new PutRecordRequest()
                .withStreamName(stream)
                .withPartitionKey(s"partitionKey-$recordNum")
                .withData(ByteBuffer.wrap(recordIterator.next().getBytes()))

              // Put the record onto the stream and capture the PutRecordResult
              kinesisClient.putRecord(putRecordRequest)
            }

            // Sleep for a second
            Thread.sleep(1000)
            println(s"Thread-$i sends $recordsPerSecond records")
          }
        }
      }).start
    }
  }
}
