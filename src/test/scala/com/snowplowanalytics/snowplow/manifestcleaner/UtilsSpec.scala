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

// specs2
import org.specs2.Specification

import Utils.DeduplicationTriple

class UtilsSpec extends Specification { def is = s2"""
    lineToTriple must return correct result without predefined etlTstamp $e1
    lineToTriple must return correct result with predefined etlTstamp $e1
  """

  def e1 = {
    val event =
      s"""blog	web	2017-07-20 18:47:06.000	2016-11-27 07:16:07.000	2017-07-20 18:47:06.000	page_view	1799a90f-f570-4414-b91a-b0db8f39cc2e		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.154.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT	ru	1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	e79bef64f3185e9d7c10d5dfdf27b9a3	"""

    val expected = DeduplicationTriple("1799a90f-f570-4414-b91a-b0db8f39cc2e", "e79bef64f3185e9d7c10d5dfdf27b9a3", 1500576426)
    Utils.lineToTriple(None)(event) must beEqualTo(expected)
  }

  def e2 = {
    val event =
      s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	1799a90f-f570-4414-b91a-b0db8f39cc2e		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.154.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT	ru	1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	e79bef64f3185e9d7c10d5dfdf27b9a3	"""

    val time = Utils.getEtlTime(Some("2017-07-20-14-44-13")).toOption.get

    val expected = DeduplicationTriple("1799a90f-f570-4414-b91a-b0db8f39cc2e", "e79bef64f3185e9d7c10d5dfdf27b9a3", 1500561853)
    Utils.lineToTriple(time)(event) must beEqualTo(expected)
  }
}
