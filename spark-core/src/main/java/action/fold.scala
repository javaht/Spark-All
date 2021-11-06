package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object fold {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 6, 4, 3),4)


 //折叠操作，aggregate 的简化版操作   分区间和分区内的计算规则一样
 val fd: Int = rdd.fold(0)(_ + _)

    println(fd)




    sc.stop()


  }

}
