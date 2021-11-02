package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

      //coalesce:   第一个参数为重分区的数目，第二个为是否进行shuffle，默认为false;
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2);
    val coaRdd: RDD[Int] = rdd.coalesce(4);


    coaRdd.collect().foreach(println)

    println("partitions: "+coaRdd.partitions.size)

    /*coaRdd.collect().foreach(println)*/


    sc.stop()

  }

}
