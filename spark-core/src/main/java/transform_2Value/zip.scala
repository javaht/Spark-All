package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object zip {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //将两个 RDD 中的元素，以键值对的形式进行合并。其中，键值对中的 Key 为第 1 个 RDD中的元素，Value 为第 2 个 RDD 中的相同位置的元素。
    //要求：两个rdd的元素个数相同  rdd的类型要求一致
     val rdd1 = sc.makeRDD(List(1,2,3,4),2)
      val rdd2 = sc.makeRDD(List(5,6,3,4),2)


      val dataRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)





     dataRDD.collect().foreach(println)


    sc.stop()
  }

}
