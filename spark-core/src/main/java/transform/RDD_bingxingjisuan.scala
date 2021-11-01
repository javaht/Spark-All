package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_bingxingjisuan {
  def main(args: Array[String]): Unit = {
    val  sparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val maprdd: RDD[Int] = rdd.map(
      num => {
        println("num ============== " + num)
        num
      }
    )

    val maprdd1: RDD[Int] = maprdd.map(
      num => {
        println("num ##############" + num)
        num
      }
    )
    maprdd1.collect()


    sc.stop()
  }

}
