package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object filter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 4, 6), 2)

    //filter  筛选 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。
    val filterRdd: RDD[Int] = rdd.filter(_ % 2 == 0) //筛选出能够整除2的数据

    filterRdd.collect().foreach(println)


    sc.stop()

  }

}
