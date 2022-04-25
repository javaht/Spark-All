package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object takeOrdered {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 6, 4, 3, 1), 2)


    //返回该 RDD 排序后的前 n 个元素组成的数组
    val takeOrder: Array[Int] = rdd.takeOrdered(3)

    println(takeOrder.mkString(","))


    sc.stop()


  }

}
