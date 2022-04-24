package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6, 7, 8)))

    val flatRdd: RDD[Int] = rdd.flatMap(list => list)


    //flatmap：就是把整体拆开后再使用  比如计算hello spark   hello java中 hello出现的次数

    flatRdd.collect().foreach(println)


    sc.stop()

  }

}
