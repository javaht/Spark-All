package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

  object sample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 1)


    //感觉这个算子就是用来抽奖的 第一个参数的意思是抽出后是否放回  第二个参数的意思是抽出的几率
    val sampleRdd: RDD[Int] = rdd.sample(false, 0.8)


    sampleRdd.collect().foreach(println)


    sc.stop()

  }

}
