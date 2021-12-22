package com.zht.Resume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

//这个是恢复数据
object wordCount_Close {
  def main(args: Array[String]): Unit = {


    val ssc = StreamingContext.getActiveOrCreate("cp", () =>{
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      ssc.checkpoint("cp")
      val lines = ssc.socketTextStream("192.168.20.62", 9999)
      val wordToOne = lines.map((_, 1))

      wordToOne.print()
         ssc
    })
 ssc.checkpoint("cp")


    ssc.start()
    ssc.awaitTermination() //阻塞main线程
  }

}
