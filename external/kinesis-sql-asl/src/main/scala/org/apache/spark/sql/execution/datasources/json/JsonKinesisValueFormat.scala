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

package org.apache.spark.sql.execution.datasources.json

import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.kinesis.KinesisValueFormat
import org.apache.spark.sql.types.StructType

/**
 * Provides access to JSON (one object per line) stream data in Amazon Kinesis.
 *
 * This function will read the input stream data at from the earliest to current time to determine
 * the input schema if `inferSchema` is enabled. To avoid taking time to read these data,
 * disable `inferSchema` option or specify the schema explicitly using [[userSpecifiedSchema]].
 *
 * You can set the following JSON-specific options to deal with non-standard JSON files:
 * <ul>
 * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
 * <li>`prefersDecimal` (default `false`): infers all floating-point values as a decimal
 * type. If the values do not fit in decimal, then it infers them as doubles.</li>
 * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
 * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
 * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
 * </li>
 * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
 * (e.g. 00012)</li>
 * <li>`allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all
 * character using backslash quoting mechanism</li>
 * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
 * during parsing.
 *   <ul>
 *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts
 *     the malformed string into a new field configured by `columnNameOfCorruptRecord`. When
 *     a schema is set by user, it sets `null` for extra fields.</li>
 *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
 *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
 *   </ul>
 * </li>
 * <li>`columnNameOfCorruptRecord` (default is the value specified in
 * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
 * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
 * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
 * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
 * date type.</li>
 * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
 * indicates a timestamp format. Custom date formats follow the formats at
 * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
 * </ul>
 */
private[spark] class JsonKinesisValueFormat extends KinesisValueFormat {

  override def inferSchema(
      sparkSession: SparkSession,
      recordRdd: RDD[Array[Byte]],
      options: Map[String, String]): StructType = {
    val parsedOptions: JSONOptions = new JSONOptions(options)
    val columnNameOfCorruptRecord =
      parsedOptions.columnNameOfCorruptRecord
        .getOrElse(sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val jsonSchema = InferSchema.infer(
      recordRdd.map(new String(_, StandardCharsets.UTF_8)),
      columnNameOfCorruptRecord,
      parsedOptions)

    checkConstraints(jsonSchema)
    jsonSchema
  }

  private def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to JSON format")
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      schema: StructType,
      options: Map[String, String]): Iterator[Array[Byte]] => Iterator[InternalRow] = {
    val parsedOptions: JSONOptions = new JSONOptions(options)
    val columnNameOfCorruptRecord = parsedOptions.columnNameOfCorruptRecord
      .getOrElse(sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    new (Iterator[Array[Byte]] => Iterator[InternalRow]) with Serializable {

      lazy val parser = new JacksonParser(schema, columnNameOfCorruptRecord, parsedOptions)

      override def apply(records: Iterator[Array[Byte]]): Iterator[InternalRow] = {
        records.flatMap(bytes => parser.parse(new String(bytes, StandardCharsets.UTF_8)))
      }
    }
  }
}
