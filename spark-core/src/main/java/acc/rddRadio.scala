package acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddRadio {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4),
      ("b", 5),
      ("c", 6)
    ))







    sc.stop()

  }

}
