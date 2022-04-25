package MyPartice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ComplexNeed {

  def main(args: Array[String]): Unit = {

    //数据汇总为学生ID，姓名，大数据成绩，数学成绩，总分，平均分。

    val conf: SparkConf = new SparkConf().setAppName("TwoScoreAvg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("xuehao.txt")







    sc.stop()
  }

}
