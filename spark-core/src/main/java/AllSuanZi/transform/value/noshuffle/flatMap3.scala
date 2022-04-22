package AllSuanZi.transform.value.noshuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMap3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //flatmap：就是把整体拆开后再使用  比如计算hello spark   hello java中 hello出现的次数
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))


    val flatRdd: RDD[Any] = rdd.flatMap(
      //这个叫模式匹配
      data => {
        data match {
          case list: List[_] => list
          case data => List(data)
        }
      }
    )


    flatRdd.collect().foreach(println)


    sc.stop()

  }

}
