package transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupBy2 {
  def main(args: Array[String]): Unit = {
    val  sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello","spark", "Hello","scala"), 2)

          //分组和分区没有必然的关系 比如所有的单词首字母都一样就是一个分组 两个分区
         //group by存在shuffle操作



    //根据首写字母分组
    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))





    groupRdd.collect().foreach(println)


    sc.stop()

  }

}
