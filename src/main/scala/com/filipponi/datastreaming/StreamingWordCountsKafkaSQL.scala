package com.filipponi.datastreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

//simple word counts of a stream from kafka
object StreamingWordCountsKafkaSQL extends App {

  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

  val checkpointDirectory = "src/test/resources/checkpoint/streamingwordcountskafkasql"

  private val createCheckpointedContext = (checkPointDir: String) => {
    () => {

      val ssc = new StreamingContext(conf, Seconds(1))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("topicA")

      val consumerRecordStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      val words: DStream[String] = consumerRecordStream.map(_.value())

      //word count executed using sql
      words.foreachRDD {
        rdd =>

          // Get the singleton instance of SparkSession
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          import spark.implicits._

          // Convert RDD[String] to DataFrame
          val wordsDataFrame = rdd.toDF("word")

          // Create a temporary view
          wordsDataFrame.createOrReplaceTempView("words")

          // Do word count on DataFrame using SQL and print it
          val wordCountsDataFrame: DataFrame =
            spark.sql("select word, count(*) as total from words group by word")

          wordCountsDataFrame.show(200)


      }


      ssc.checkpoint(checkPointDir) // set checkpoint directory, looks like need to be done as last thing?
      ssc
    }
  }

  //the operation of the streaming context is already encodeded in the checkpoint!
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createCheckpointedContext(checkpointDirectory))

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate

}