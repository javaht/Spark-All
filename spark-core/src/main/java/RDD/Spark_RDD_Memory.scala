package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Memory {
  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
    val sc = new SparkContext(sparkConf)

      //从内存中创建RDD  将内存中集合的数据作为处理的数据源

      val seq = Seq[Int](1,2,3,4)
       //val rdd: RDD[Int] = sc.parallelize(seq)
       val rdd: RDD[Int] = sc.makeRDD(seq)
     rdd.collect().foreach(println)



    sc.stop()
  }
   
  

}
