package com.zht.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


//foreachRDD 不会出现时间戳  底层是对rdd进行操作
object wordCount_OutPut {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("cp")
    val lines = ssc.socketTextStream("192.168.20.62",9999)
    val wordToOne = lines.map((_, 1))

    //窗口默认是滑动的 默认是一个采集周期进行滑动可能会出现重复数据  为了避免这种 可以改变滑动的幅度

    //当窗口的范围比较大 但是滑动幅度比较少 可以采用增加数据和删除数据的方式  无需重复计算
    val windowDs = wordToOne.reduceByKeyAndWindow(
      (x:Int,y:Int) =>{ x+y }, //这个是增加的
      (x:Int,y:Int) =>{ x-y },
      Seconds(9), //采集周期 窗口的范围=采集周期的整数倍
      Seconds(3)) //是滑动的周期=采集周期的整数倍




    val wordCount = windowDs.reduceByKey(_ + _)
//foreachRDD 不会出现时间戳  底层是对rdd进行操作
    wordCount.foreachRDD(
      rdd=>{
        rdd
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
