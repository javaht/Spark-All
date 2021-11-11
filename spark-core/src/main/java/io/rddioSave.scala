package io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddioSave {

  def main(args: Array[String]): Unit = {



    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")



    sc.stop()
  }



}
