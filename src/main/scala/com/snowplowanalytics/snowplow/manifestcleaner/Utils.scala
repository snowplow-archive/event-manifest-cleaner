/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.manifestcleaner

// scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.control.NonFatal

// Java
import java.nio.charset.StandardCharsets.UTF_8

// Jackson
import com.fasterxml.jackson.databind.{ ObjectMapper, JsonNode }

// Iglu client
import com.snowplowanalytics.iglu.client.Validated
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Joda time
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatterBuilder

/**
  * Util functions and constant that not directly related to spark job
  */
object Utils {

  // Column indexes in TSV
  val EtlTimestampIndex = 2
  val EventIdIndex = 6
  val FingerprintIndex = 129

  private val UrlSafeBase64 = new Base64(true) // true means "url safe"

  private lazy val Mapper = new ObjectMapper

  /**
    * Data extracted from EnrichedEvent and storing in DynamoDB
    */
  case class DeduplicationTriple(eventId: String, fingerprint: String, etlTstamp: String)

  /**
    * Split `EnrichedEvent` TSV line and extract necessary columns
    *
    * @param line plain `EnrichedEvent` TSV
    * @return deduplication triple encapsulated into special class
    */
  def lineToTriple(line: String): DeduplicationTriple = {
    val tsv = line.split('\t')
    try {
      DeduplicationTriple(eventId = tsv(EventIdIndex), etlTstamp = tsv(EtlTimestampIndex), fingerprint = tsv(FingerprintIndex))
    } catch {
      case e: IndexOutOfBoundsException => throw new RuntimeException(s"ERROR: Cannot split TSV [$line]\n${e.toString}")
    }
  }

  /**
    * Converts a base64-encoded JSON string into a JsonNode
    *
    * @param str base64-encoded JSON
    * @return a JsonNode on Success, a NonEmptyList of ProcessingMessages on Failure
    */
  def base64ToJsonNode(str: String): Validated[JsonNode] =
    (for {
      raw  <- decodeBase64Url(str)
      node <- extractJson(raw)
    } yield node).toProcessingMessage

  def extractJson(instance: String): Validation[String, JsonNode] =
    try {
      Mapper.readTree(instance).success
    } catch {
      case NonFatal(e) => s"Invalid JSON [%s] with parsing error: %s".format(instance, e.getMessage).failure
    }

  def decodeBase64Url(str: String): Validation[String, String] = {
    try {
      val decodedBytes = UrlSafeBase64.decode(str)
      new String(decodedBytes, UTF_8).success
    } catch {
      case NonFatal(e) =>
        "Exception while decoding Base64-decoding string [%s] (URL-safe encoding): [%s]".format(str, e.getMessage).failure
    }
  }
}
