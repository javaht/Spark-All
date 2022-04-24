package AllSuanZi.transform.twoValue.ByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //将数据根据不同的规则进行分区内计算和分区间计算。注意和reduceBykey(分区内和分区间的规则一样)比较
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    //aggregateByKey存在函数柯里化 有两个参数列表
    // 第二个参数列表中第一个参数是分区内计算规则  第二个参数是分区间计算规则      math.min(x,y) 两者取最小
    //第一个()中的值 主要是为了和第一个数作比较
    val aggRdd: RDD[(String, Int)] = rdd.aggregateByKey(0, 0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    // val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _+_ )



    println("这个是分区：  " + aggRdd.partitions.size)


    aggRdd.collect().foreach(println)

    sc.stop()
  }

}
