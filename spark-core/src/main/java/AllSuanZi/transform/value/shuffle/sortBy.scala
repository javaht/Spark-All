package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

    //这是一个排序的算子。 第二个参数代表是否正序排序(默认为 true)。第三个参数代表分区的个数。
    val rdd: RDD[Int] = sc.makeRDD(List(1, 5, 6, 4, 8, 9, 10, 35), 4);
    val sortRdd: RDD[Int] = rdd.sortBy(
      num => num,
      false,
      4
    )



    sortRdd.collect().foreach(println)
    println("partitions: " + sortRdd.partitions.size)

    sc.stop()

  }

}
