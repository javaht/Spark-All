package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object first {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)

    //数据源中的第一个元素
    val first: Int = rdd.first()

    println(first)


    sc.stop()


  }

}
