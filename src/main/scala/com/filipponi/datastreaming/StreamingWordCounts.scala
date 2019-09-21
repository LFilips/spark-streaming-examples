package com.filipponi.datastreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object StreamingWordCounts extends App {

  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

  val checkpointDirectory = "src/test/resources/checkpoint/streamingwordcount"

  private val createCheckpointedContext = (checkPointDir: String) => {
    () => {
      val ssc = new StreamingContext(conf, Seconds(1))

      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      val words: DStream[String] = lines.flatMap(_.split(" "))

      // Count each word in each batch
      val pairs = words.map(word => (word, 1))
      val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

      val updateFunction: (Seq[Int], Option[Int]) => Option[Int] = (newValues, runningCount) => {
        //my state is (word,count) but i don't have to care to do the look up of the key
        //since this function will be called for every new element and the key lookup will be performed
        //by spark, so what i just need is The sequence of new value for that key and the previous value
        //that is an Option[] because is not guaranteed that there is one
        val newCount = newValues.sum + runningCount.getOrElse(0) // add the new values with the previous running count to get the new count
        Some(newCount)
      }

      //in my case the state will be defined as a cumulative maps with the word and counts
      val runningCounts = wordCounts.updateStateByKey(updateFunction)
      runningCounts.checkpoint(Seconds(1))

      //printing the first 200 keys
      runningCounts.print(200)
      ssc.checkpoint(checkPointDir) // set checkpoint directory, looks like need to be done as last thing?
      ssc
    }
  }

  //the operation of the streaming context is already encodeded in the checkpoint!
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createCheckpointedContext(checkpointDirectory))

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate

}