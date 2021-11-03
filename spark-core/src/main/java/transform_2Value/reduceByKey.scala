package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

       //这个是根据相同的key 进行分组
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3),("d",4)),4)

    val reduceRdd: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    println("这个是分区：  "+reduceRdd.partitions.size)

    reduceRdd.collect().foreach(println)


    sc.stop()
  }

}