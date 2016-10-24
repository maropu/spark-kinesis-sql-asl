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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.source.libsvm.LibSVMKinesisValueFormat
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.datasources.csv.CSVKinesisValueFormat
import org.apache.spark.sql.execution.datasources.json.JsonKinesisValueFormat
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class KinesisDataFormatSuite extends SparkFunSuite with SharedSQLContext {

  private val emptyOptions = Map.empty[String, String]

  test("defualt") {
    val inputRdd = spark.sparkContext
      .parallelize(Seq("abcdef", "ghijklmn", "opqrstu"))
      .map(_.getBytes(StandardCharsets.UTF_8))

    val defaultFormat = new DefaultKinesisValueFormat()
    val inferredSchema = defaultFormat.inferSchema(spark, inputRdd, emptyOptions)
    assert(inferredSchema === StructType(StructField("value", BinaryType) :: Nil))

    val passThroughReadFunc = defaultFormat.buildReader(spark, inferredSchema, emptyOptions)
    val rdd = inputRdd.mapPartitions(passThroughReadFunc).map { r =>
      r.toSeq(inferredSchema).map {
        case b: Array[Byte] => new String(b, StandardCharsets.UTF_8)
        case other => other
      }
    }
    assert(rdd.collect.toSet === Set(Seq("abcdef"), Seq("ghijklmn"), Seq("opqrstu")))
  }

  test("csv") {
    val inputRdd = spark.sparkContext
      .parallelize(Seq("abc,1,0.3", "def,3,0.8", ",0,2.8,jkl"))
      .map(_.getBytes(StandardCharsets.UTF_8))

    val csvFormat = new CSVKinesisValueFormat()
    val defaultSchema = csvFormat.inferSchema(spark, inputRdd, Map("inferSchema" -> "false"))
    assert(defaultSchema ===
      StructType(
        StructField("_c0", StringType) ::
        StructField("_c1", StringType) ::
        StructField("_c2", StringType) ::
        Nil
      )
    )
    val inferredSchema = csvFormat.inferSchema(spark, inputRdd, Map("inferSchema" -> "true"))
    assert(inferredSchema ===
      StructType(
        StructField("_c0", StringType) ::
        StructField("_c1", IntegerType) ::
        StructField("_c2", DoubleType) ::
        Nil
      )
    )

    val csvReadFunc = csvFormat.buildReader(spark, inferredSchema, emptyOptions)
    val rdd = inputRdd.mapPartitions(csvReadFunc).map { r =>
      r.toSeq(inferredSchema).map {
        case utf8: UTF8String => utf8.toString
        case other => other
      }
    }
    assert(rdd.collect.toSet ===
      Set(Seq("abc", 1, 0.3), Seq("def", 3, 0.8), Seq(null, 0, 2.8)))
  }

  test("json") {
    val inputRdd = spark.sparkContext
      .parallelize(
        Seq(
          """{"c0": "abc", "c1": 1, "c2": 0.3}""",
          """{"c0": "def", "c1": 3}""",
          """{"c0": "", "c1": 0, "c2": 2.8, "c3": "ghi"}"""
        ))
      .map(_.getBytes(StandardCharsets.UTF_8))

    val jsonFormat = new JsonKinesisValueFormat()
    val inferredSchema = jsonFormat.inferSchema(spark, inputRdd, emptyOptions)
    assert(inferredSchema ===
      StructType(
        StructField("c0", StringType) ::
        StructField("c1", LongType) ::
        StructField("c2", DoubleType) ::
        StructField("c3", StringType) ::
        Nil
      )
    )

    val jsonReadFunc = jsonFormat.buildReader(spark, inferredSchema, emptyOptions)
    val rdd = inputRdd.mapPartitions(jsonReadFunc).map { r =>
      r.toSeq(inferredSchema).map {
        case utf8: UTF8String => utf8.toString
        case other => other
      }
    }
    assert(rdd.collect.toSet ===
      Set(Seq("abc", 1, 0.3, null), Seq("def", 3, null, null), Seq("", 0, 2.8, "ghi")))
  }

  test("libsvm") {
    val inputRdd = spark.sparkContext
      .parallelize(Seq("1.0 1:0.1 3:0.5 8:0.1", "0.0 2:0.5", "0.0 4:0.3 9:0.9"))
      .map(_.getBytes(StandardCharsets.UTF_8))

    val libSVMFormat = new LibSVMKinesisValueFormat()
    val libSVMOptions = Map("numFeatures" -> "64", "vectorType" -> "sparse")
    val inferredSchema = libSVMFormat.inferSchema(spark, inputRdd, libSVMOptions)
    // assert(inferredSchema ===
    //   StructType(
    //     StructField("label", DoubleType, false) ::
    //     StructField("features", new VectorUDT(), false) ::
    //     Nil
    //   )
    // )

    val libSVMReadFunc = libSVMFormat.buildReader(spark, inferredSchema, libSVMOptions)
    val rdd = inputRdd.mapPartitions(libSVMReadFunc).map(_.toSeq(inferredSchema))
    assert(rdd.collect.toSet === Set(
      Seq(1.0, new SparseVector(64, Array(0, 2, 7), Array(0.1, 0.5, 0.1))),
      Seq(0.0, new SparseVector(64, Array(1), Array(0.5))),
      Seq(0.0, new SparseVector(64, Array(3, 8), Array(0.3, 0.9)))
    ))
  }
}
