package RddToDataFrame

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object dataFrameTest1 {
  //直接指定列名
  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("MyTest").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val textrdd: RDD[String] = spark.sparkContext.textFile("hdfs://192.168.2.240:9000//user//hive//warehouse//ods.db//ods_ac01", 1)

    val value: RDD[Array[String]] = textrdd.map(_.split("\001"))
    import spark.implicits._
    val frame: DataFrame = value.map(
      column => (column(3), column(6))
    ).toDF("name", "age")

    frame.show()

    spark.stop()

  }

}
