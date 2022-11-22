package AllSuanZi.transform.twoValue.ByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    //GroupByKey 只能分组，不能聚合
    //reduceBykey包含分组和聚合的功能
    // reduceByKey可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的数据量

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)), 4)
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey(2)


    println("这个是分区：  " + groupRdd.partitions.size)


    groupRdd.collect().foreach(println)

    sc.stop()
  }

}
