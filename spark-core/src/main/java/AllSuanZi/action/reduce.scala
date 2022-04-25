package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)

    //行动算子会触发作业(job)的执行的方法
    //底层调用的是环境对象的runjob方法
    //底层代码会创建ActiveJob  并提交执行

    println(rdd.reduce(_ + _)) //两两聚合

    println(rdd.reduce((x, y) => x + y)) //两两聚合

    sc.stop()


  }

}
