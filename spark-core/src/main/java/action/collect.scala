package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object collect {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),4)


      //collect  会将不同分区的数据按照分区顺序采集到Driver端内存中 姓成数组
    val ints: Array[Int] = rdd.collect()

    println(ints.mkString(","))



    sc.stop()


  }

}
