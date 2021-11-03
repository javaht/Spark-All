package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object partitionBy {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //这个算子针对键值对进行重新分区。
    // partitionBy会根据指定的分区规则(默认是HashPartitioner)对数据进行重新分区.
    // 它是PairRDDFunctions类中的方法！不是RDD的方法。
    // 记得和repartitions与coalesce进行对比
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "111"), (2, "bbb"), (3, "ccc"), (4, "ddd")),2)

            rdd.collect().foreach(println)

   println("========================================================")
   val rdd2: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    rdd2.collect().foreach(println)




    sc.stop()
  }

}
