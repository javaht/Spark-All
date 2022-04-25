package MyPartice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScoreTopTen {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("JiSuanScore").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val stuRDD: RDD[String] = sc.textFile("datas/score.txt")
    val mapRDD: RDD[(String, (Int, String, String))] = stuRDD.map(line => {
      val info: Array[String] = line.split(",")
      val id = info(0)
      val name = info(1)
      val score = info(2).toInt
      val stuclass = info(3)
      (stuclass, (score, id, name))

    })


    val gbkRDD: RDD[(String, Iterable[(Int, String, String)])] = mapRDD.groupByKey()


    //这段是精髓 上边的groupByKey()方法可以将数据按照key进行分组，但是不能按照value进行分组

    val value: RDD[(String, List[(Int, String, String)])] = gbkRDD.mapValues(
      values => values.toList.sortBy(_._1).reverse.take(3)
    )


    value.foreach(println)


    sc.stop()
  }

}
