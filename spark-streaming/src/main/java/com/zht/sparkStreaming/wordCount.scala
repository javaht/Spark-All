package com.zht.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object wordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(4))


    val line = ssc.socketTextStream("192.168.20.62", 9999)

    val words = line.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))
    val wordToCount = wordToOne.reduceByKey(_ + _)

    wordToCount.print()

      //由于sparkstream采集器是长期执行的任务 所以不能直接关闭
     //ssc.stop()
    //1.启动采集器 2.等待采集器的执行

    ssc.start()
    ssc.awaitTermination()
  }

}
