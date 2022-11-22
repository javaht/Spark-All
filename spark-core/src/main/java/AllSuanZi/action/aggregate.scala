package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregate {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 5, 5), 2)

    //分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合

    // aggregateBykey 第一个值只会参与分区内计算
    //aggregate     第一个值不止参与分区内计算 还有分区间的计算
    val aggregat: Int = rdd.aggregate(10)(_ + _, _ + _)

    println(aggregat)

    sc.stop()


  }

}
