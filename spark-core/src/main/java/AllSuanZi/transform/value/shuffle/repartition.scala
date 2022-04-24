package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)
    //repartition和coalesce之间的区别在于是否shuffle操作  coalesce的第二个参数是是否开启shuffle  默认是false
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2);
    val reRdd: RDD[Int] = rdd.repartition(4)

    //重新分区的根本是通过hash取模后再分区,因此必须通过shuffle
    val value: RDD[(Int, Int)] = reRdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(iter => (iter, index))
      }

    )
    value.collect().foreach(println)


    //reRdd.collect().foreach(println)

    println("partitions: " + reRdd.partitions.size)


    sc.stop()

  }

}
