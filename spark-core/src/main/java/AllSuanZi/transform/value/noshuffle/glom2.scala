package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object glom2 {
  def main(args: Array[String]): Unit = {

    //glom:将同一个分区的数据转换为相同类型的内存数组进行处理
    //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    //这一步负责把List(1,2,3,4) 分成两个分区的数据  一个是包含12的数组  一个是包含34的数组
    val glomRdd: RDD[Array[Int]] = sc.makeRDD(List(1, 2, 3, 4), 2).glom()

    //这一步负责取出每个数组中的最大值
    val maxRdd: RDD[Int] = glomRdd.map(
      data => data.max
    )
    println(maxRdd.collect().sum)


    sc.stop()
  }
}
