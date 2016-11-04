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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types._

private[kinesis] class KinesisSourceProvider extends StreamSourceProvider
    with DataSourceRegister with Logging {

  import KinesisSource._

  /**
   * Returns the name and schema of the source.
   */
  override def sourceSchema(
      sqlContext: SQLContext,
      userSpecifiedSchema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val usedSchema = userSpecifiedSchema.map(withTimestamp).getOrElse {
      KinesisSource.inferSchema(sqlContext, parameters)
    }
    (shortName(), usedSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      userSpecifiedSchema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    new KinesisSource(
      sqlContext,
      metadataPath,
      userSpecifiedSchema.map(withTimestamp),
      parameters)
  }

  override def shortName(): String = "kinesis"
}
