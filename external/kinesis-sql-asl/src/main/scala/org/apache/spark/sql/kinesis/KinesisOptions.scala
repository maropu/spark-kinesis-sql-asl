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

import java.text.SimpleDateFormat
import java.util.Date

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap

private[spark] class KinesisOptions(@transient private val parameters: Map[String, String])
    extends Logging with Serializable {

  @transient private val caseSensitiveParams = new CaseInsensitiveMap(parameters)

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = caseSensitiveParams.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param == "true") {
      true
    } else if (param == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = caseSensitiveParams.get(paramName)
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

  val streamNames = {
    parameters.get("streams").map(_.split(",")).getOrElse {
      throw new IllegalArgumentException("Option `streams` must be set")
    }
  }

  val endpointUrl = parameters.getOrElse("endpointUrl",
    throw new IllegalArgumentException("Option `endpointUrl` must be set"))

  val regionId = parameters.getOrElse("regionName",
    RegionUtils.getRegionMetadata.getRegionByEndpoint(endpointUrl).getName())

  val checkpointName = parameters.getOrElse("checkpointName", {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    s"kinesis-checkpoint-${streamNames.sorted.mkString("-")}-${sdf.format(new Date())}"
  })

  val format = caseSensitiveParams.get("format").map {
    case format if Set("csv", "json", "libsvm").contains(format) => format
    case unknownFormat => throw new IllegalArgumentException(s"Unknown format: $unknownFormat")
  }.getOrElse {
    "default"
  }

  val initialPositionInStream = caseSensitiveParams.get("initialPositionInStream").map {
    case "latest" => InitialPositionInStream.LATEST
    case "earliest" | "trim_horizon" => InitialPositionInStream.TRIM_HORIZON
    case unknownPos => throw new IllegalArgumentException(s"Unknown position: $unknownPos")
  }.getOrElse {
    InitialPositionInStream.LATEST
  }

  val softLimitMaxRecordsPerTrigger = getInt("softLimitMaxRecordsPerTrigger", -1)

  val purgeIntervalMs = getInt("purgeInternalMs", 300 * 1000) // 5min by default

  val retryTimeoutMs = getInt("retryTimeoutMs", 10000)

  val failOnDataLoss = getBool("failOnDataLoss", false)
}
