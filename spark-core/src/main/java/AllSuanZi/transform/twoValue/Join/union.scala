package AllSuanZi.transform.twoValue.Join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object union {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    // 当两个RDD的数据类型一致的时候 union取并集  不去重
    //当两个rdd数据类型不一致的时候  不能使用
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.makeRDD(List(5, 6, 1, 2), 2)
    val dataRDD: RDD[Int] = rdd1.union(rdd2)


    dataRDD.collect().foreach(println)


    sc.stop()
  }

}
