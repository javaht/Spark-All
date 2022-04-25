package MyPartice

import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScoreIs100 {
  def main(args: Array[String]): Unit = {

      val conf: SparkConf = new SparkConf().setAppName("ScoreIs100").setMaster("local")
      val sc = new SparkContext(conf)

    val textRdd: RDD[String] = sc.textFile("datas/xueHaoWithScore.txt")


//取出成绩单科成绩为100的学生的学号

    textRdd.map(
      line=>{
        val datas: Array[String] = line.split(",")
        (datas(0),datas(2).toInt)
      }
    ).filter(
      t=>{
        t._2 == 100
      }
    ).distinct(1).foreach(println)



    sc.stop()
  }

}
