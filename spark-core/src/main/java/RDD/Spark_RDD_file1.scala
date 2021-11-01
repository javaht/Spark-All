package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_file1 {
  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
    val sc = new SparkContext(sparkConf)

      //从文件中创建RDD  将文件中的数据作为处理的数据源

    val rdd = sc.wholeTextFiles("data/1.txt")
     rdd.collect().foreach(println)

    sc.stop()
  }
   
  

}
