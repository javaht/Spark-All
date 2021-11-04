package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform_txt {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.textFile("data/1.txt")
    val mapRdd: RDD[String] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(2)
      }
    )

    mapRdd.collect().foreach(println)


    sparkContext.stop()
  }

}
