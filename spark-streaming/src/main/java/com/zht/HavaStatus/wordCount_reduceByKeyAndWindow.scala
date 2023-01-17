package com.zht.HavaStatus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//这个窗口看图了解合适
object wordCount_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

   ssc.checkpoint("cp")
    val lines = ssc.socketTextStream("192.168.20.62",9999)
    val wordToOne = lines.map((_, 1))

    //窗口默认是滑动的 默认是一个采集周期进行滑动可能会出现重复数据  为了避免这种 可以改变滑动的幅度

    //当窗口的范围比较大 但是滑动幅度比较少 可以采用增加数据和删除数据的方式  无需重复计算

/**
 * 窗口操作优化的机制
 * 比如说我们还是每隔五秒查看一下前十五秒数据，我们可以加上新进来的批次，再减去出去的批次，防止任务堆积
 * 用优化机制必须设置checkpoint，不设置会报错
 * 第一个参数是先加上新来的批次
 * 第二个参数是减去出去的批次
 * 第三个参数是窗口长度
 * 第四个参数是滑动间隔
 **/
    val windowDs = wordToOne.reduceByKeyAndWindow(
      (x:Int,y:Int) =>{ x+y }, //这个是增加的
      (x:Int,y:Int) =>{ x-y },//这个是减少的
      Seconds(9), //采集周期 窗口的范围=采集周期的整数倍
      Seconds(3) //是滑动的周期=采集周期的整数倍
    )


    val wordCount = windowDs.reduceByKey(_ + _)
    wordCount.print()


      ssc.start()
    ssc.awaitTermination()
  }

}
