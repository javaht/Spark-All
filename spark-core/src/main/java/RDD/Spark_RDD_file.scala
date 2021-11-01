package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_file {
  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
    val sc = new SparkContext(sparkConf)

      //从文件中创建RDD  将文件中的数据作为处理的数据源

    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test3.txt")//可以写绝对路径 也可写相对路径

     rdd.collect().foreach(println)



    sc.stop()
  }
   
  

}
