package MyPartice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ComplexNeed {

  def main(args: Array[String]): Unit = {



    val conf: SparkConf = new SparkConf().setAppName("TwoScoreAvg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val xuehaoRdd: RDD[String] = sc.textFile("datas/xuehao.txt")
    val scoreRdd: RDD[String] = sc.textFile("datas/xueHaoWithScore.txt")

    val xuehaoMapRdd: RDD[(String, String)] = xuehaoRdd.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1))
      }
    )

    val scoreMapRdd: RDD[(String, (String, String))] = scoreRdd.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), (datas(1), datas(2)))
      }
    )
    //数据汇总为学生ID，姓名，大数据成绩，数学成绩，总分，平均分。
    val joinRdd: RDD[(String, Iterable[(String, String, String)])] = xuehaoMapRdd.join(scoreMapRdd).map(
      line => {
        val xuhao: String = line._1
        val name: String = line._2._1
        val bigData: String = line._2._2._1
        val math: String = line._2._2._2
        (xuhao, (name, bigData, math))
      }
    ).groupByKey(1)

   // joinRdd.map().foreach(println)








    sc.stop()
  }

}
