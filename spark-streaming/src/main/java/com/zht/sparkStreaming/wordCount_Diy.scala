package com.zht.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

//自定义数据采集器
object wordCount_Diy {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    val messageDs = ssc.receiverStream(new myReceiver)

    messageDs.print()

    ssc.start()
    ssc.awaitTermination()
  }
  /*
  * 自定义数据采集器
  * 1.继承Receiver  定义泛型  参数为存储级别
  * */
  class myReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg =true;
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg){
            val message ="采集的数据为："+new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()

    }

    override def onStop(): Unit = {

      flg =false
    }
  }

}
