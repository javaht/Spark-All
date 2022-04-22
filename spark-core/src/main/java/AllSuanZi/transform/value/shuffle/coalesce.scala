package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

    //coalesce:   第一个参数为重分区的数目，第二个为是否进行shuffle，默认为false;
    //当不允许shuffle(第二个参数为flase)的时候 就不能扩大分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 4);
    val coaRdd: RDD[Int] = rdd.coalesce(8, true);

    coaRdd.collect().foreach(println)

    println("分区数为  : " + coaRdd.partitions.size)





    /*coaRdd.collect().foreach(println)*/


    sc.stop()

  }

}
