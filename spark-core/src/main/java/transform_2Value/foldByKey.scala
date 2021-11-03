package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object foldByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //分区内和分区间的计算规则一样  就用foldByKey  相当于是aggregateBykey的特殊情况
    //
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3),("a",4)),2)

    val foldRdd: RDD[(String, Int)] = rdd.foldByKey(0)(
      (x, y) => x + y //等同于 _+_
    )




    println("这个是分区：  "+foldRdd.partitions.size)


    foldRdd.collect().foreach(println)

    sc.stop()
  }

}
