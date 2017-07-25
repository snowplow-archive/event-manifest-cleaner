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

// Scala
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

// AWS
import com.amazonaws.auth.{ BasicAWSCredentials, AWSStaticCredentialsProvider }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }

// JSON
import org.json4s.{ JValue, DefaultFormats }
import org.json4s.jackson.JsonMethods.fromJsonNode
import com.fasterxml.jackson.databind.JsonNode

// Iglu
import com.snowplowanalytics.iglu.client.{ Resolver, Validated, ValidatedNel }
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._

/**
  * Common trait for duplicate storages, storing triple of event attributes,
  * allowing to identify duplicated events across batches.
  * Currently, single implementation is `DynamoDbStorage`.
  * This module is a port of `com.snowplowanalytics.snowplow.enrich.hadoop.DuplicateStorage`
  * from `3-enrich/scala-hadoop-shred`
  */
trait DuplicateStorage {

  /**
    * Try to delete parts of the event into duplicates table
    *
    * @param eventId snowplow event id (UUID)
    * @param eventFingerprint enriched event's fingerprint
    * @param etlTstamp timestamp of enrichment job
    * @return true if event is successfully deleted from table,
    *         false if condition is failed -
   *          record with these eventId and finterprint had different etlTstamp
    */
  def delete(eventId: String, eventFingerprint: String, etlTstamp: Int): Boolean
}


/**
  * Companion object holding ADT for possible configurations and concrete implementations
  */
object DuplicateStorage {

  /**
    * ADT to hold all possible types for duplicate storage configurations
    */
  sealed trait DuplicateStorageConfig

  /**
    * Configuration required to use duplicate-storage in dynamodb
    * This instance supposed to correspond to `iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/1-0-*`
    * Class supposed to used only for extracting and carrying configuration.
    * For actual interaction `DuplicateStorage` should be used
    *
    * @param name arbitrary human-readable name for storage target
    * @param accessKeyId AWS access key id
    * @param secretAccessKey AWS secret access key
    * @param awsRegion AWS region
    * @param dynamodbTable AWS DynamoDB to delete duplicates
    */
  case class DynamoDbConfig(name: String, accessKeyId: String, secretAccessKey: String, awsRegion: String, dynamodbTable: String) extends DuplicateStorageConfig

  object DynamoDbConfig {

    implicit val formats = DefaultFormats

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON,
      * just extracted from CLI arguments (therefore wrapped in `Validation[Option]`
      * Lifted version of `extractFromBase64`
      *
      * @param base64Config base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param igluResolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extract(base64Config: Validated[Option[String]], igluResolver: ValidatedNel[Resolver]): ValidatedNel[Option[DynamoDbConfig]] = {
      val nestedValidation = (base64Config.toValidationNel |@| igluResolver) { (config: Option[String], resolver: Resolver) =>
        config match {
          case Some(encodedConfig) =>
            for { config <- extractFromBase64(encodedConfig, resolver) } yield config.some
          case None => none.successNel
        }
      }

      nestedValidation.flatMap(identity)
    }

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON.
      * Also checks that JSON is valid
      *
      * @param base64 base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param resolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extractFromBase64(base64: String, resolver: Resolver): ValidatedNel[DynamoDbConfig] = {
      Utils.base64ToJsonNode(base64)  // Decode
        .toValidationNel                                                                 // Fix container type
        .flatMap { node: JsonNode => node.validate(dataOnly = true)(resolver) }          // Validate against schema
        .map(fromJsonNode)                                                               // Transform to JValue
        .flatMap { json: JValue =>                                                       // Extract
        Validation.fromTryCatch(json.extract[DynamoDbConfig]).leftMap(e => toProcMsg(e.getMessage)).toValidationNel
      }
    }
  }

  /**
    * Initialize storage object from configuration.
    * This supposed to be a universal constructor for `DuplicateStorage`,
    *
    * @param config all configuration required to instantiate storage
    * @return valid storage if no exceptions were thrown
    */
  def initStorage(config: DuplicateStorageConfig): Validated[DuplicateStorage] =
    config match {
      case DynamoDbConfig(_, accessKeyId, secretAccessKey, awsRegion, tableName) =>
        try {
          val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
          val client = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(awsRegion)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build()
          new DynamoDbStorage(client, tableName).success
        } catch {
          case NonFatal(e) =>
            toProcMsg("Cannot initialize duplicate storage:\n" + Option(e.getMessage).getOrElse("")).failure
        }
    }

  /**
    * Primary duplicate storage object, supposed to handle interactions with DynamoDB table
    * These objects encapsulate DynamoDB client, which contains lot of mutable state,
    * references and unserializable objects - therefore it should be constructed on last step -
    * straight inside `ShredJob`. To initialize use `initStorage`
    *
    * @param client AWS DynamoDB client object
    * @param table DynamodDB table name with duplicates
    */
  class DynamoDbStorage private[manifestcleaner](client: AmazonDynamoDB, table: String) extends DuplicateStorage {
    import DynamoDbStorage._

    def delete(eventId: String, eventFingerprint: String, etl: Int): Boolean = {
      val deleteRequest = deleteRequestDummy
        .withExpressionAttributeValues(Map(":tst" -> new AttributeValue().withN(etl.toString)).asJava)
        .withKey(
          attributeValues(List(eventIdColumn -> eventId)),
          attributeValues(List(fingerprintColumn -> eventFingerprint))
        )

      try {
        client.deleteItem(deleteRequest)
        true
      } catch {
        case _: ConditionalCheckFailedException => false
      }
    }

    /**
      * Dummy request required with conditional delete iff duplicate was created
      * during currently processing ETL
      */
    private val deleteRequestDummy: DeleteItemRequest = new DeleteItemRequest()
      .withTableName(table)
      .withConditionExpression(s"$etlTstampColumn = :tst")
  }

  object DynamoDbStorage {

    val eventIdColumn = "eventId"
    val fingerprintColumn = "fingerprint"
    val etlTstampColumn = "etlTime"

    /**
      * Helper method to transform list arguments into DynamoDB-compatible item with its
      * attributes
      *
      * @param attributes list of pairs of names and values, where values are string
      *                    and integers only
      * @return Java-compatible Hash-map
      */
    private[manifestcleaner] def attributeValues(attributes: Seq[(String, Any)]): java.util.Map.Entry[String, AttributeValue] =
      try {
        attributes.toMap.mapValues(asAttributeValue).asJava.entrySet().asScala.head
      } catch {
        case NonFatal(e) =>
          throw new RuntimeException("Invalid Java to Scala conversion of DynamoDB attributes\n" + e.getMessage)
      }

    /**
      * Convert **only** string and integer to DynamoDB-compatible object
      */
    private def asAttributeValue(v: Any): AttributeValue = {
      val value = new AttributeValue
      v match {
        case s: String => value.withS(s)
        case n: java.lang.Number => value.withN(n.toString)
        case _ => null
      }
    }
  }
}
