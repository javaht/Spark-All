package com.zht.Close

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

//这个窗口看图了解合适
object wordCount_Close {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val lines = ssc.socketTextStream("192.168.20.62",9999)
    val wordToOne = lines.map((_, 1))

    wordToOne.print()


      ssc.start()//启动

      //如果想要关闭采集器 需要创建线程  而且需要在第三方程序中增加关闭状态

    //这个是例子
/*    new Thread(
      new Runnable {
        override def run(): Unit ={
           //mysql  redis等
          while(true){
            if(true){
              //获取sparkstreaming的状态
              val state = ssc.getState()
              if(state==StreamingContextState.ACTIVE){
                //优雅关闭  计算节点不再接收新的数据 而是将现有的数据处理完毕然后关闭
                ssc.stop(true,true)//第二个参数就是优雅关闭
              }
              Thread.sleep(5000)
            }
          }


        }
      }
    )*/

     Thread.sleep(5000)
    val state = ssc.getState()
    if(state==StreamingContextState.ACTIVE){
      //优雅关闭  计算节点不再接收新的数据 而是将现有的数据处理完毕然后关闭
      ssc.stop(true,true)//第二个参数就是优雅关闭
    }

    ssc.awaitTermination() //阻塞main线程
  }

}
