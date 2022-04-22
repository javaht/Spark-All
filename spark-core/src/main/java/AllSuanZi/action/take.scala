package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object take {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)


    //返回一个由 RDD 的前 n 个元素组成的数组
    val take: Array[Int] = rdd.take(3)

    println(take.mkString(","))


    sc.stop()


  }

}
