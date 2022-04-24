package AllSuanZi.transform.twoValue.Join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rightOuterJoin {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //类似于 SQL 语句的右外连接
    val dataRDD1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val dataRDD2 = sc.makeRDD(List(("a", 3), ("a", 5), ("d", 3)))
    val rdd: RDD[(String, (Option[Int], Int))] = dataRDD1.rightOuterJoin(dataRDD2)

    rdd.collect().foreach(println)


    /*   这个是结果

*/

    sc.stop()


  }

}
