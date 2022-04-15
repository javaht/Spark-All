package RddToDataFrame

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object dataFrameTest3 {
  //编程方式定义schema
  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("MyTest").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val textrdd: RDD[String] = spark.sparkContext.textFile("hdfs://192.168.2.240:9000//user//hive//warehouse//ods.db//ods_ac01", 1)
    val data: RDD[Array[String]] = textrdd.map(_.split("\001"))

    val value: RDD[Row] = data.map(
      column => Row(column(3), column(6).toInt)
    )

    val schema = StructType(Seq(
      StructField("name", StringType, true),
      StructField("sex", IntegerType, true)
    ))

    val frame: DataFrame = spark.createDataFrame(value, schema)


    frame.show()

    spark.stop()

  }
}
