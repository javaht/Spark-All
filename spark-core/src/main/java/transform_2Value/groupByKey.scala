package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

       //groupByKey这个是根据key 进行分组  但是并不做聚合操作
      //reduceBykey包含分组和聚合的功能 GroupByKey 只能分组，不能聚合
    //reduceByKey可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的数据量
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3),("d",4)),4)
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey(2)


    println("这个是分区：  "+groupRdd.partitions.size)


    groupRdd.collect().foreach(println)

    sc.stop()
  }

}
