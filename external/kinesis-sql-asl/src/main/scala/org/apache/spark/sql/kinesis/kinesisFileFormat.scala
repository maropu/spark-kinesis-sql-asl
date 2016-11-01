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

import org.apache.spark.ml.source.libsvm.LibSVMKinesisValueFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.CSVKinesisValueFormat
import org.apache.spark.sql.execution.datasources.json.JsonKinesisValueFormat
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

trait KinesisValueFormat {

  /**
   * Used to infer the schema of data read from Amazon Kinesis.
   */
  def inferSchema(
      sparkSession: SparkSession,
      recordRdd: RDD[Array[Byte]],
      options: Map[String, String]): StructType

  /**
   * Returns a serializable closure that can convert Amazon Kinesis input binary data
   * into SparkSQL [[InternalRow]].
   */
  def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow]
}

/**
 * Regards Amazon Kinesis input data as SparkSQL [[BinaryType]].
 */
private[kinesis] class DefaultKinesisValueFormat extends KinesisValueFormat {

  import DefaultKinesisValueFormat._

  override def inferSchema(
      sparkSession: SparkSession,
      recordRdd: RDD[Array[Byte]],
      options: Map[String, String]): StructType = {
    defaultSchema
  }

  override def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {
      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        records.map(InternalRow(_))
      }
    }
  }
}

private[kinesis] object DefaultKinesisValueFormat {

  val defaultSchema: StructType = StructType(
    StructField("value", BinaryType) ::
    Nil
  )
}

private[kinesis] object KinesisDataFormatFactory {

  def create(formatName: String): KinesisValueFormat = formatName match {
    case "csv" => new CSVKinesisValueFormat()
    case "json" => new JsonKinesisValueFormat()
    case "libsvm" => new LibSVMKinesisValueFormat()
    case "default" => new DefaultKinesisValueFormat()
  }
}
