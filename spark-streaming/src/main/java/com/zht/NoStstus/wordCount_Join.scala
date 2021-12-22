package com.zht.NoStstus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordCount_Join {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val data9999 = ssc.socketTextStream("192.168.20.62",9999)
    val data8888 = ssc.socketTextStream("192.168.20.62",8888)
        //
        val map9999 = data9999.map((_, 9))

    val map8888 = data8888.map((_, 8))
    val joinDs = map9999.join(map8888)   //join操作其实就是两个rdd的join


    joinDs.print()
      ssc.start()
    ssc.awaitTermination()
  }

}
