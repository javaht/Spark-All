package MyPartice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//取出成绩排名前5的学生成绩信息
object SumScoreTop5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SumScoreTop5").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreRdd: RDD[String] = sc.textFile("datas/xueHaoWithScore.txt")

    val reduceRdd: RDD[(String, Int)] = scoreRdd.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(2).toInt)
      }
    ).reduceByKey(_ + _)

     //方法1： 这里的top底层代码其实是takeOrderBy  然后跟着一个reverse
    //reduceRdd.top(5)(Ordering.by(_._2)).foreach(println)
    //方法2：
   // reduceRdd.sortBy(_._2,false).take(5).foreach(println)





   sc.stop()
  }

}
