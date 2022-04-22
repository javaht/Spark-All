package wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddAcc {

  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //获取系统的累加器   spark默认提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum") //起名叫做sum

    //使用累加器
    // 少加：如果没有行动算子的话 那么不会执行
    //多加：转换算子多次调用累加器 就会累加(可以)
    ///一般情况下 累加器放置在行动算子中进行操作
    rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )


    println(sumAcc.value)


    sc.stop()
  }

}
