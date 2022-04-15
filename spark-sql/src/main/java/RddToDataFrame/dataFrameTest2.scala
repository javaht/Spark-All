package RddToDataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object dataFrameTest2 {

  def main(args: Array[String]): Unit = {

    //这种反射机制推断包含特定类型对象的Schema信息
    var conf: SparkConf = new SparkConf().setAppName("MyTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    val textrdd: RDD[String] = sc.textFile("hdfs://192.168.2.240:9000//user//hive//warehouse//ods.db//ods_ac01", 1)

    val data: RDD[Array[String]] = textrdd.map(_.split("\001"))

    val value: RDD[Person] = data.map(
      x => Person(x(3), x(6).toInt)
    )

    import spark.implicits._
    val df: DataFrame = value.toDF()


    // df.filter(df("aac004")>=2).show()
    df.createTempView("person")
    spark.sql("select * from person where aac004=2").show()


    sc.stop()


  }

  case class Person(aaa103: String, aac004: Int)

}
