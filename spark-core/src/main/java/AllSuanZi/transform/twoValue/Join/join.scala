package AllSuanZi.transform.twoValue.Join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object join {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的(K,(V,W))的 RDD
    //key不同的不会出现在结果中    key相同会形成元组
    //多个的会挨个匹配
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (1, 5), (3, 6)))
    rdd.join(rdd1).collect().foreach(println)


    sc.stop()


  }

}
