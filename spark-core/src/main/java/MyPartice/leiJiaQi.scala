package MyPartice

import org.apache.spark.{SparkConf, SparkContext}

object leiJiaQi {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("累加器").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4),1)
    val sumAcc = sc.longAccumulator("sumAcc")

   rdd.foreach(
      num => (sumAcc.add(num))
    )
    println(sumAcc.value)




    sc.stop()
  }

}
