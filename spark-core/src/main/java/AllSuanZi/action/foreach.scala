package AllSuanZi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object foreach {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //特殊现象就是  分布式遍历 RDD 中的每一个元素，调用指定函数

    rdd.collect().foreach(println) //这里的foreach 其实是driver端内存集合的循环遍历方法

    println("=========================================================")
    rdd.foreach(println) //这里的foreach其实是excutor端内存数据打印
    sc.stop()



    //算子 ：Operator(操作)
    // RDD的方法和scala集合对象的方法不一样
    //集合的方法都是在同一个节点的内存中完成
    //RDD的方法可以将计算逻辑发送到Excutor(分布式节点) 执行
    //为了区分不同的处理效果 所以将RDD的方法称之为算子
    //RDD的方法外部的操作都是在driver端执行的  内部逻辑代码是在Excutor端执行-  bb

  }

}
