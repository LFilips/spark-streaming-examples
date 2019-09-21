package com.filipponi.datastreaming

import org.scalatest.FlatSpec

class StreamingWordCountsKafkaSQLTest extends FlatSpec {

  "StreamingWordCountsKafkaSQL" should "count the words coming from kafka" in {

     val kafkaTopic = "topicA"

      StreamingWordCountsKafka.main(null)

  }

}
