package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object glom {
  def main(args: Array[String]): Unit = {

    //将同一个分区的数据转换为相同类型的内存数组进行处理
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val glomRdd: RDD[Array[Int]] = rdd.glom()

    glomRdd.collect().foreach(
      data => println(data.mkString(","))
    )


    sc.stop()
  }
}
