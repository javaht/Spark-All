package rddRely

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddBlood {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkconf)

     //  A====>B=====>C         A依赖B  B依赖C  A间接依赖C
     // RDD里面叫做血缘关系



     //toDebugString 可以看到血缘关系
    val fileRDD: RDD[String] = sc.textFile("datas/apache.log")
    println(fileRDD.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)


    resultRDD.collect()


    sc.stop()

  }

}
