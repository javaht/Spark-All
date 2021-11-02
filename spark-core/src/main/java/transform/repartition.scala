package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object repartition {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)
    //repartition和coalesce之间的区别在于是否shuffle操作  coalesce的第二个参数是是否开启shuffle  默认是false
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 8);
    val reRdd: RDD[Int] = rdd.repartition(2)

    val value: RDD[Int] = reRdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (0 == index) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    value.collect().foreach(println)


    //reRdd.collect().foreach(println)

    println("partitions: "+reRdd.partitions.size)




    sc.stop()

  }

}
