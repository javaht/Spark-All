package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object distinct {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 4, 5), 1)
    val disRdd: RDD[Int] = rdd.distinct()


      //distinct的实现和下方实现原理相同
 /*   val rdd: RDD[String] = sc.makeRDD(Array("hello", "spark", "hello", "java"),1)
      val disRdd: RDD[String] = rdd.map {(_,1)}.reduceByKey(_ + _).map(_._1)
 */



 /*
    hello    (_,1)       (hello,1)          reduceBykey(_+_)      (hello,2)    map(_._1)
    spark  ========>     (spark,1)===============================>(spark,1)==============> hello  spark  java
    hello                (hello,1)                                (java,1)
    java                 (java,1)
*/




    disRdd.collect().foreach(println)


    sc.stop()

  }

}




/*

一共有两种抽取方法：
1.抽取数据不放回（伯努利算法）
   伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
   具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
   第一个参数：抽取的数据是否放回,false：不放回
   第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
   第三个参数：随机数种子
2. 抽取数据放回（泊松算法）
   第一个参数：抽取的数据是否放回，true：放回；false：不放回
   第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
   第三个参数：随机数种子

   */
