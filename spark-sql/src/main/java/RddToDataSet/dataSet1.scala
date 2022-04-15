package RddToDataSet

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object dataSet1 {
  //使用样例类
  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("MyTest").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val textrdd: RDD[String] = spark.sparkContext.textFile("hdfs://192.168.2.240:9000//user//hive//warehouse//ods.db//ods_ac01", 1)

    import spark.implicits._
    val value: RDD[Array[String]] = textrdd.map(_.split("\001"))

     value.map(
      x => Person(x(3).toString, x(6).toInt)
    ).toDS().show()




  }
  case class Person(name: String, age: Int)
}
