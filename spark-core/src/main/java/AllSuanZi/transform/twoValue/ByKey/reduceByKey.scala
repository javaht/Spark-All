package AllSuanZi.transform.twoValue.ByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //相同的key的数据 进行value数据的聚合操作
    //scala语言中 一般的聚合操作都是两两聚合 spark的聚合也是
    val dataRDD1 = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)), 4)
    val reduceRdd: RDD[(String, Int)] = dataRDD1.reduceByKey(_ + _)

//    val reduceRdd2 = dataRDD1.reduceByKey(_ + _, 1)
//    reduceRdd2.collect().foreach(println)
    reduceRdd.collect().foreach(println)
    println("partitions: " + reduceRdd.partitions.size)


    sc.stop()
  }

}
