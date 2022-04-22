package transform.transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object subtract {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

      //这个算子的意思是得到交集后 去除交集 取剩下的值(比如a.subtract(b)，a-(a和b的交集)   )
     val rdd1 = sc.makeRDD(List(1,2,3,4),2)
      val rdd2 = sc.makeRDD(List(5,6,1,2),2)

    val dataRDD: RDD[Int] = rdd1.subtract(rdd2)



     dataRDD.collect().foreach(println)


    sc.stop()
  }

}
