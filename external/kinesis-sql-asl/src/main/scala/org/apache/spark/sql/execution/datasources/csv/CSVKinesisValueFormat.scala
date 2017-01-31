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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.kinesis.KinesisValueFormat
import org.apache.spark.sql.types._

/**
 * Provides access to CSV stream data in Amazon Kinesis.
 *
 * This function will read the input stream data at from the earliest to current time to determine
 * the input schema if `inferSchema` is enabled. To avoid taking time to read these data,
 * disable `inferSchema` option or specify the schema explicitly using [[userSpecifiedSchema]].
 *
 * You can set the following CSV-specific options to deal with CSV files:
 * <ul>
 * <li>`sep` (default `,`): sets the single character as a separator for each
 * field and value.</li>
 * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
 * type.</li>
 * <li>`quote` (default `"`): sets the single character used for escaping quoted values where
 * the separator can be part of the value. If you would like to turn off quotations, you need to
 * set not `null` but an empty string. This behaviour is different form
 * `com.databricks.spark.csv`.</li>
 * <li>`escape` (default `\`): sets the single character used for escaping quotes inside
 * an already quoted value.</li>
 * <li>`comment` (default empty string): sets the single character used for skipping lines
 * beginning with this character. By default, it is disabled.</li>
 * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
 * requires one extra pass over the data.</li>
 * <li>`ignoreLeadingWhiteSpace` (default `false`): defines whether or not leading whitespaces
 * from values being read should be skipped.</li>
 * <li>`ignoreTrailingWhiteSpace` (default `false`): defines whether or not trailing
 * whitespaces from values being read should be skipped.</li>
 * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
 * 2.0.1, this applies to all supported types including the string type.</li>
 * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
 * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
 * value.</li>
 * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
 * value.</li>
 * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
 * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
 * date type.</li>
 * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
 * indicates a timestamp format. Custom date formats follow the formats at
 * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
 * `java.sql.Timestamp.valueOf()` and `java.sql.Date.valueOf()` or ISO 8601 format.</li>
 * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
 * a record can have.</li>
 * <li>`maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed
 * for any given value being read. By default, it is -1 meaning unlimited length</li>
 * <li>`maxMalformedLogPerPartition` (default `10`): sets the maximum number of malformed rows
 * Spark will log for each partition. Malformed records beyond this number will be ignored.</li>
 * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
 *    during parsing.
 *   <ul>
 *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record. When
 *       a schema is set by user, it sets `null` for extra fields.</li>
 *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
 *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
 *   </ul>
 * </li>
 * </ul>
 */
private[spark] class CSVKinesisValueFormat extends KinesisValueFormat {

  override def inferSchema(
      sparkSession: SparkSession,
      recordRdd: RDD[Array[Byte]],
      options: Map[String, String]): StructType = {
    val csvOptions = new CSVOptions(options)
    val rowRdd = recordRdd.mapPartitionsInternal { part =>
      val csvReader = new CsvReader(csvOptions)
      part.map { line =>
        csvReader.parseLine(new String(line, csvOptions.charset))
      }
    }
    val header = rowRdd.first.zipWithIndex.map { case (_, index) =>
      s"_c${index}"
    }
    if (csvOptions.inferSchemaFlag) {
      CSVInferSchema.infer(rowRdd, header, csvOptions)
    } else {
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName =>
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable with Logging {
      lazy val lineReader = new CsvReader(csvOptions)
      lazy val rowParser = CSVRelation.csvParser(schema, schema.fieldNames, csvOptions)

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        records.flatMap { r =>
          val recordStr = new String(r, csvOptions.charset)
          val record = lineReader.parseLine(recordStr)
          if (record != null) {
            rowParser(record, 0)
          } else {
            /**
             * When spark receives unacceptable byte arrays from Kinesis streams,
             * this code path is executed.
             */
            logWarning(s"Can't parse an input record: $recordStr")
            None
          }
        }
      }
    }
  }
}
