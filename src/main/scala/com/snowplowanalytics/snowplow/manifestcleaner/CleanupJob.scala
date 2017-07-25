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

// Spark
import org.apache.spark.{SparkConf, SparkContext}

// This project
import Main.JobConf
import Utils._

object CleanupJob {

  /**
    * Store event's duplication triple in DynamoDB
    * Can throw exception, that could short-circuit whole job
    */
  def delete(triple: DeduplicationTriple, storage: DuplicateStorage): Boolean = {
    storage.delete(eventId = triple.eventId, eventFingerprint = triple.fingerprint, etlTstamp = triple.etlTstamp)
  }

  def run(jobConfig: JobConf) = {

    lazy val storage = DuplicateStorage.initStorage(jobConfig.storageConfig).fold(
      e => throw new RuntimeException(e.toString),
      r => r)

    val config = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master", "local[*]")
      .set("spark.hadoop.io.compression.codecs", classOf[R83Codec].getCanonicalName)

    val sc = new SparkContext(config)
    val deletedTotal = sc.longAccumulator("Deleted events")
    val skippedTotal = sc.longAccumulator("Skipped events")

    val events = sc.textFile(s"s3a://${jobConfig.enrichedInBucket}part-*")
    events.map(lineToTriple(jobConfig.orphanEtlTime)).foreach { triple =>
      val deleted = delete(triple, storage)
      if (deleted) {
        deletedTotal.add(1)
      } else {
        skippedTotal.add(1)
      }
    }

    println(s"Event Manifest Cleaner deleted ${deletedTotal.value} events. ${skippedTotal.value} events were skipped")
  }
}
