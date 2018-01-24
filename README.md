# Event Manifest Cleaner

## Introduction

This is an [Apache Spark][spark] job to cleanup a Snowplow event manifest in DynamoDB for particular Enrich job result.
This job solves the problem, where Shred job half populates event manifest, but pipeline failed downstream.
If engineer will try to recover pipeline by running Shred job again - most of events will be mistakenly marked as duplicates which will lead to data loss in Redshift.

## Usage

Detailed usage case can be found in [Recovering pipelines with cross-batch deduplication][recovery-tutorial].

In order to use Event Manifest Cleaner, you need to have [boto2][boto]
installed:

```
$ pip install boto
```

Now you can run Event Manifest Cleaner with a single command (inside
event-manifest-cleaner directory):

```
$ python run.py run_emr $ENRICHED_EVENTS_DIR $STORAGE_CONFIG_PATH $IGLU_RESOLVER_PATH
```

The user running it should have the _dynamodb:DeleteTable_ rights for the related table.

Task has three required arguments: 

1. Path to enriched events directory. This can be not archived directory in 
   `enriched.good` or in rare cases particular directory in `enriched.archive`.
2. Local path to [Duplicate storage][dynamodb-config] configuration JSON
3. Local path to [Iglu resolver][resolver] configuration JSON

Optionally, you can also pass following options:

* `--time` ETL time for orphan enriched events
* `--log-path` to store EMR job logs on S3. Normally, Manifest Cleaner does not
  produce any logs or output, but if some error occured you'll be able to
  inspect it in EMR logs stored in this path.
* `--profile` to specify AWS profile to create this EMR job.
* `--jar` to specify S3 path to custom JAR


## Building

Assuming git, **[Vagrant][vagrant-install]** and **[VirtualBox][virtualbox-install]** installed:

```bash
host$ git clone https://github.com/snowplow/event-manifest-cleaner
host$ cd event-manifest-cleaner
host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ sbt assembly
```

## Copyright and License

Copyright 2017 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[spark]: http://spark.apache.org/

[boto]: http://boto.cloudhackers.com/en/latest/

[recovery-tutorial]: https://discourse.snowplowanalytics.com/t/recovering-pipelines-with-cross-batch-deduplication-enabled-tutorial/1397?source_topic_id=1225

[config]: https://github.com/snowplow/snowplow/blob/master/3-enrich/emr-etl-runner/config/config.yml.sample
[resolver]: https://github.com/snowplow/iglu/wiki/Iglu-client-configuration
[shredding]: https://github.com/snowplow/snowplow/wiki/Shredding

[dynamodb-config]: https://github.com/snowplow/snowplow/wiki/Configuring-storage-targets#dynamodb

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[license]: http://www.apache.org/licenses/LICENSE-2.0
