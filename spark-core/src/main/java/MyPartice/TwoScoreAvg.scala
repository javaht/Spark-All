package MyPartice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TwoScoreAvg {
  def main(args: Array[String]): Unit = {

     //求成绩的平均值
    val conf: SparkConf = new SparkConf().setAppName("TwoScoreAvg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("datas/xueHaoWithScore.txt")
    rdd.map(
      line =>{
      val datas = line.split(",")
        (datas(0),datas(2).toInt)
      }
    ).reduceByKey(_+_).map(
      t=>{
        (t._1,t._2/2)
      }
    ).foreach(println)




    sc.stop()
  }

}
