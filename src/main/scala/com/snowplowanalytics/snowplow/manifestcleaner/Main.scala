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

// Scalaz
import scalaz._
import Scalaz._

// Joda time
import org.joda.time.DateTime

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// JSON Schema Validator
import com.github.fge.jsonschema.core.report.ProcessingMessage

// Iglu client
import com.snowplowanalytics.iglu.client.{Resolver, ValidatedNel}

// This project
import DuplicateStorage._
import generated.ProjectMetadata

object Main {

  /**
    * Required prefix for full S3 paths
    */
  val S3Prefixes = Set("s3://", "s3a://", "s3n://")

  /**
    * Raw CLI arguments
    */
  private[manifestcleaner] case class RawJobConf(
    enrichedInBucket: String,
    b64StorageConfig: String,
    b64ResolverConfig: String,
    orphanEtlTime: Option[String])

  private[this] val rawJobConf = RawJobConf("", "", "", None)

  /**
    * Parsed job configuration with normalized values
    *
    * @param enrichedInBucket enriched archive
    * @param storageConfig parsed and validated DynamoDB
    */
  case class JobConf(enrichedInBucket: String, storageConfig: DuplicateStorageConfig, orphanEtlTime: Option[Int])

  private val parser = new scopt.OptionParser[RawJobConf](ProjectMetadata.name) {
    head(generated.ProjectMetadata.name, generated.ProjectMetadata.version)

    opt[String]('i', "run-folder").required()
      .action((x, c) => c.copy(enrichedInBucket = normalizeBucket(x)))
      .text("S3 path to enriched data failed to shred")
      .validate { s3path =>
        val fail = failure(s"--run-folder S3 path should start with valid prefix ${S3Prefixes.mkString(", ")}")
        S3Prefixes.foldLeft(fail) { (acc, pref) =>
          if (s3path.startsWith(pref)) success
          else if (acc.isRight) acc
          else fail
        }
      }

    opt[String]('t', "time").action((x, c) =>
      c.copy(orphanEtlTime = Some(x))).text("Time to substitute actual etl time from enriched events (YYYY-mm-dd-HH-MM-SS)")

    opt[String]('c', "storage-config").required().action((x, c) =>
      c.copy(b64StorageConfig = x)).text("Base64-encoded AWS DynamoDB storage configuration JSON")

    opt[String]('r', "resolver").required().action((x, c) =>
      c.copy(b64ResolverConfig = x)).text("Base64-encoded Iglu resolver configuration JSON")
  }

  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(Success(config)) => CleanupJob.run(config)
      case Some(Failure(failureNel)) =>
        val message = s"Unable to parse arguments: \n${failureNel.list.mkString("\n")}"
        sys.error(message)
      case None => sys.exit(1)
    }
  }

  /**
    * Parse command line arguments into job configuration object
    */
  def parse(args: Array[String]): Option[ValidatedNel[JobConf]] =
    parser.parse(args, rawJobConf).map(transform)

  /**
    * Transform raw CLI options into normalized configuration double checking its values
    */
  def transform(jobConf: RawJobConf): ValidatedNel[JobConf] = jobConf match {
    case RawJobConf(enrichedInBucket, b64StorageConfig, b64ResolverConfig, time) =>
      val validTime: ValidatedNel[Option[Int]] = Utils.getEtlTime(time)
      val storage = for {
        resolverConfig <- Utils.base64ToJsonNode(b64ResolverConfig).toValidationNel[ProcessingMessage, JsonNode]
        resolver <- Resolver.parse(resolverConfig)
        storageConfig <- DynamoDbConfig.extractFromBase64(b64StorageConfig, resolver)
      } yield storageConfig

      (storage |@| validTime) { (storageConfig, time) =>
        JobConf(enrichedInBucket, storageConfig, time)
      }
  }

  /**
    * Drop s3 protocol and make sure there's trailing slash
    */
  private def normalizeBucket(bucket: String): String = {
    val schemaless = S3Prefixes.foldLeft(None: Option[String]) { (acc, pref) =>
      if (bucket.startsWith(pref)) Some(bucket.stripPrefix(pref))
      else if (acc.nonEmpty) acc
      else None
    }.getOrElse(throw new RuntimeException(s"Invalid S3 bucket path $bucket"))
    if (schemaless.endsWith("/")) schemaless else schemaless + "/"
  }
}
