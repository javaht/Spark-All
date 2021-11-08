package rddRely

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddRely {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkconf)

     //dependencies  可以把相邻的两个rdd之间的关系展示出来
    val fileRDD: RDD[String] = sc.textFile("datas/apache.log")
    println(fileRDD.dependencies)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)


    resultRDD.collect()


    sc.stop()

  }

}
