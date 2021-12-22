package com.zht.HavaStatus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//这个窗口看图了解合适
object wordCount_window {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val lines = ssc.socketTextStream("192.168.20.62",9999)
    val wordToOne = lines.map((_, 1))

    //窗口默认是滑动的 默认是一个采集周期进行滑动可能会出现重复数据  为了避免这种 可以改变滑动的幅度

    val windowDs = wordToOne.window(Seconds(6),Seconds(6)) //第一个参数为采集周期 窗口的范围=采集周期的整数倍  第二个参数是滑动的周期=采集周期的整数倍




    val wordCount = windowDs.reduceByKey(_ + _)
    wordCount.print()


      ssc.start()
    ssc.awaitTermination()
  }

}
