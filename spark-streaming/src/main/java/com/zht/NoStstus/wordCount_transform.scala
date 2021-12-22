package com.zht.NoStstus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordCount_transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    val lines = ssc.socketTextStream("192.168.20.62",9999)


    //transform 将底层的rdd获取到后进行操作
    //使用场景：1.Dstream功能不完善  2.需要代码周期性的执行
    val newDs = lines.transform(
      rdd => rdd.map(
        //Code:Driver端(周期性执行 一个RDD一次)
          str =>{
            //Code:Excuter
            str
          }
    ))

    lines.map(
      data=>{
        //Code:Excuter
        data
      }
    )



    //state.print()
      ssc.start()
    ssc.awaitTermination()
  }

}
