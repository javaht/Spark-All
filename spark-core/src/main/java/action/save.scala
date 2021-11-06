package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object save {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    // 保存成 Text 文件
    rdd.saveAsTextFile("output")
/*    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")
    // 保存成 Sequencefile 文件
    rdd.map((_,1)).saveAsSequenceFile("output2")*/


    sc.stop()


  }

}
