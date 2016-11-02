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

package org.apache.spark.ml.source.libsvm

import java.io.IOException
import java.nio.charset.StandardCharsets

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.kinesis.KinesisValueFormat
import org.apache.spark.sql.types._

/**
 * Provides access to LibSVM stream data in Amazon Kinesis.
 *
 * This function will read the input stream data at from the earliest to current time to determine
 * the input schema if `inferSchema` is enabled. To avoid taking time to read these data,
 * disable `inferSchema` option or specify the schema explicitly using [[userSpecifiedSchema]].
 *
 * You can set the following LibSVM-specific options to deal with LibSVM files:
 * <ul>
 * <li>`vectorType` (default `sparse`): sets the vector type of features.</li>
 * <li>`numFeatures`: sets the number of features. If a user doesn't specify a valid one,
 * it infers the number on runtime.</li>
 * </ul>
 */
private[spark] class LibSVMKinesisValueFormat extends KinesisValueFormat {

  override def inferSchema(
      sparkSession: SparkSession,
      recordRdd: RDD[Array[Byte]],
      options: Map[String, String]): StructType = {
    val numFeatures: Int = options.get("numFeatures").map(_.toInt).filter(_ > 0).getOrElse {
      val parsed = recordRdd.map { record =>
        MLUtils.parseLibSVMRecord(new String(record, StandardCharsets.UTF_8))
      }
      MLUtils.computeNumFeatures(parsed)
    }

    val featuresMetadata = new MetadataBuilder()
      .putLong("numFeatures", numFeatures)
      .build()

    StructType(
      StructField("label", DoubleType, nullable = false) ::
        StructField("features", new VectorUDT(), nullable = false, featuresMetadata) ::
        Nil)
  }

  private def verifySchema(dataSchema: StructType): Unit = {
    if (
      dataSchema.size != 2 ||
        !dataSchema(0).dataType.sameType(DataTypes.DoubleType) ||
        !dataSchema(1).dataType.sameType(new VectorUDT()) ||
        !(dataSchema(1).metadata.getLong("numFeatures").toInt > 0)
    ) {
      throw new IOException(s"Illegal schema for libsvm data, schema=$dataSchema")
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): (Iterator[Array[Byte]]) => Iterator[InternalRow] = {
    verifySchema(schema)
    val numFeatures = schema("features").metadata.getLong("numFeatures").toInt
    val sparse = options.getOrElse("vectorType", "sparse") == "sparse"
    assert(numFeatures > 0)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        val points = records
          .map(new String(_, StandardCharsets.UTF_8))
          .map { line =>
          val (label, indices, values) = MLUtils.parseLibSVMRecord(line)
          LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
        }

        points.map { pt =>
          val features = if (sparse) pt.features.toSparse else pt.features.toDense
          InternalRow(pt.label, features)
        }
      }
    }
  }
}
