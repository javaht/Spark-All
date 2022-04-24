package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 4, 5), 1)
    rdd.distinct().foreach(println)


    //distinct的实现和下方实现原理相同
    /*   val rdd: RDD[String] = sc.makeRDD(Array("hello", "spark", "hello", "java"),1)
      val disRdd: RDD[String] = rdd.map {(_,1)}.reduceByKey(_ + _).map(_._1)
 */


    /*
    hello    (_,1)       (hello,1)          reduceBykey(_+_)      (hello,2)    map(_._1)
    spark  ========>     (spark,1)===============================>(spark,1)==============> hello  spark  java
    hello                (hello,1)                                (java,1)
    java                 (java,1)
*/







    sc.stop()

  }

}
