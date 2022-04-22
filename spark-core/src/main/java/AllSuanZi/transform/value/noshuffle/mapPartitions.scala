package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mpRdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    mpRdd.collect().foreach(println)


    sc.stop()

  }

}
