package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中

    //groupby 将数据源中的每一个数据进行分组判断 根据返回的分组key  相同的key值会放置在一个组中
    def groupFunction(num: Int): Int = {
      num % 2
    }

    rdd.groupBy(groupFunction).collect().foreach(println)


    sc.stop()

  }

}
