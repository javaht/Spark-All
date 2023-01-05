package dataQingXie

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.InitUtil

object sampleCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      import  session._
    val df: DataFrame = session.sql("select * from sparktuning.salecourse_detail")

//    val df: DataFrame = session.sql("select count(1) from sparktuning.salecourse_detail  where majorid='3' ")  7031352
//    df.show()


    //核心是读取某个字段后
    df.select("majorid")
      .sample(false,0.1)
      .rdd
      .map(k => (k, 1))
      .reduceByKey(_ + _)   // 统计不同key出现的次数
      .map(k => (k._2, k._1))
      .sortByKey(false) // 统计的key进行排序
      .take(10).foreach(println)




    session.close()
  }

}
