package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    val mpRdd: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (1 == index) {
          //只保留第二个分区的数据
          iter
        } else {
          Nil.iterator //这个代表空的迭代器
        }
      }
    )


    mpRdd.collect().foreach(println)


    sc.stop()

  }

}
