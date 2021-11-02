package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

      //coalesce:   第一个参数为重分区的数目，第二个为是否进行shuffle，默认为false;
      // 这个算子只能缩减分区  不能扩大分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 8);
    val coaRdd: RDD[Int] = rdd.coalesce(4);

    val value: RDD[Int] = coaRdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (0 == index) {
          //只保留第二个分区的数据
          iter
        } else {
          Nil.iterator
        }
      }
    )
    value.collect().foreach(println)

    println("partitions: "+coaRdd.partitions.size)

    /*coaRdd.collect().foreach(println)*/


    sc.stop()

  }

}
