package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Rdd_Partition {
  def main(args: Array[String]): Unit = {
    val  sparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
     val sc = new SparkContext(sparkConf)

    //makeRDD可以传递第二个参数 第二个参数是分区的数量。
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    sc.textFile("",4)

     sc.stop()
  }

}
