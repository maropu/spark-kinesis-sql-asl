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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.csv.CSVKinesisValueFormat
import org.apache.spark.streaming.dstream.DStream

final class KinesisStreamingOps(ds: DStream[Array[Byte]]) {

  def execute(schema: StructType, options: Map[String, String], f: DataFrame => DataFrame)
      (implicit spark: SparkSession): DStream[Row] = {
    val csvReader = new CSVKinesisValueFormat().buildReader(spark, schema, options)
    ds.transform[Row] { rdd: RDD[Array[Byte]] =>
      val logicalRdd = LogicalRDD(schema.toAttributes, rdd.mapPartitionsInternal(csvReader))(spark)
      val df = Dataset.ofRows(spark, logicalRdd)
      f(df).rdd
    }
  }
}

object KinesisStreamingOps {

  /**
   * Implicitly inject the [[KinesisStreamingOps]] into [[DStream]].
   */
  implicit def dataFrameToHivemallStreamingOps(ds: DStream[Array[Byte]])
    : KinesisStreamingOps = {
    new KinesisStreamingOps(ds)
  }
}
