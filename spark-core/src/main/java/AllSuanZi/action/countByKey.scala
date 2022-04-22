package AllSuanZi.action

import org.apache.spark.{SparkConf, SparkContext}

object countByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)



    /*    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val value: collection.Map[Int, Long] = rdd.countByValue()

    println(value) //表示值出现的次数
    */


    //统计每种 key 的个数
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "b"), (2, "b"), (3, "d"), (3, "c")))


    val countByVal: collection.Map[Int, Long] = rdd.countByKey()

    println(countByVal)

    // 结果 Map(1 -> 3, 2 -> 1, 3 -> 2)



    sc.stop()


  }

}
