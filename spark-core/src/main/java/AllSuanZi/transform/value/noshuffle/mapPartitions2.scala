package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitions2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    //取出每个分区的最大值
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(
      iter => (List(iter.max).iterator)
    )

    mapRdd.collect().foreach(println)


    sc.stop()
  }
}
