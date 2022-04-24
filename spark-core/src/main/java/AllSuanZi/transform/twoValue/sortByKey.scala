package AllSuanZi.transform.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sortByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


        //这个就是排序  怎么实现的还是要看源码
    val dataRDD1 = sc.makeRDD(List(("1",2),("2",6),("1",3),("3",4)))
    val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(true)
   // val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(false)

    println("这个是分区：  "+sortRDD1.partitions.size)


    sortRDD1.collect().foreach(println)

    sc.stop()
  }

}
