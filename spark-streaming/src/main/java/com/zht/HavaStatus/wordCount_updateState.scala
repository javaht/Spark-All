package com.zht.HavaStatus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordCount_updateState {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    //使用有状态操作时 需要设定检查点路径
    ssc.checkpoint("cp")
    val datas = ssc.socketTextStream("192.168.20.62",9999)
    //无状态数据操作 只对当前的采集周期内的数据进行处理  在某些场合下需要保留数据的统计状态 实现数据的汇总
    val wordToOne = datas.map((_, 1))

    //val wordToCount = wordToOne.reduceByKey(_ + _)

    //updateStateByKey  根据key对数据的状态进行更新
    // 传递的参数有两个值 第一个值表示相同的key的value数据  第二个表示缓冲区的相同key数据
    val state = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )


    state.print()
      ssc.start()
    ssc.awaitTermination()
  }

}
