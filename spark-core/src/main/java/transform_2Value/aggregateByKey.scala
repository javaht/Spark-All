package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //将数据根据不同的规则进行分区内计算和分区间计算
    //第一个参数是
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3),("d",4)),4)

    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)



    println("这个是分区：  "+value.partitions.size)


    value.collect().foreach(println)

    sc.stop()
  }

}
