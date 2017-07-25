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
import org.joda.time.{ DateTime, DateTimeZone }
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.format.DateTimeFormat


/**
  * Util functions and constant that not directly related to spark job
  */
object Utils {

  // Column indexes in TSV
  val EtlTimestampIndex = 2
  val EventIdIndex = 6
  val FingerprintIndex = 129

  /**
   * Date format in archive buckets: run=YYYY-MM-dd-HH-mm-ss
   * In EmrEtlRunner: "%Y-%m-%d-%H-%M-%S"
   */
  val timeFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss")

  /**
   * Default format (etl_tstamp, dvce_sent_tstamp, dvce_created_tstamp)
   * @see `com.snowplowanalytics.snowplow.enrich.hadoop.inputs.EnrichedEventLoader`
   */
  val RedshiftTstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)

  /**
   * Parse run id from enriched event archive dir
   *
   * @param date etl timestamp timestamp
   * @return datetime object if parsed successfully
   */
  def parseTime(date: String): Validated[DateTime]= {
    try {
      Success(DateTime.parse(date, timeFormat))
    } catch {
      case _: IllegalArgumentException =>
        Failure(s"ETL Time $date has invalid format".toProcessingMessage)
    }
  }

  private val UrlSafeBase64 = new Base64(true) // true means "url safe"

  private lazy val Mapper = new ObjectMapper

  /**
    * Data extracted from EnrichedEvent and storing in DynamoDB
    */
  case class DeduplicationTriple(eventId: String, fingerprint: String, etlTstamp: Int)

  /**
    * Split `EnrichedEvent` TSV line and extract necessary columns
    *
    * @param etlTime optional hard-coded etl time to substitute extracted from event
    * @param line plain `EnrichedEvent` TSV
    * @return deduplication triple encapsulated into special class
    */
  def lineToTriple(etlTime: Option[Int])(line: String): DeduplicationTriple = {
    val tsv = line.split('\t')
    try {
      val etl = etlTime.getOrElse((DateTime.parse(tsv(EtlTimestampIndex), RedshiftTstampFormat).getMillis / 1000).toInt)
      DeduplicationTriple(eventId = tsv(EventIdIndex), etlTstamp = etl, fingerprint = tsv(FingerprintIndex))
    } catch {
      case e: IndexOutOfBoundsException =>
        throw new RuntimeException(s"ERROR: Cannot split TSV [$line]\n${e.toString}")
    }
  }

  /** Transform YYYY-mm-dd-HH-MM-SS to milliseconds */
  def getEtlTime(time: Option[String]) =
    time.map(Utils.parseTime(_).map(t => (t.getMillis / 1000).toInt).toValidationNel).sequenceU

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
