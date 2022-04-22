package transform.transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object intersection {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

      //intersection的主要作用是求两个Rdd的交集 返回一个新的RDD.存在shuffle 如果没有交集  则返回空的rdd
     val dataRDD1 = sc.makeRDD(List(1,2,3,4),2)
      val dataRDD2 = sc.makeRDD(List(5,6,1,2),2)
      val dataRDD = dataRDD1.intersection(dataRDD2)


    //如果数据类型不一致  则不能使用
 /*   val rdd1: RDD[String] = sc.makeRDD(Array("22", "java"))
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 22))

    val dataRDD = rdd2.intersection(rdd1)
*/
     dataRDD.collect().foreach(println)


    sc.stop()
  }

}
