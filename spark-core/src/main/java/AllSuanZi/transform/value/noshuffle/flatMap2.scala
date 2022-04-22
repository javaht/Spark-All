package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMap2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //flatmap：就是把整体拆开后再使用  比如计算hello spark   hello java中 hello出现的次数
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala"))
    val flatRdd: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )


    flatRdd.collect().foreach(println)


    sc.stop()

  }

}
