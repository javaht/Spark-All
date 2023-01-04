package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object rddPersist {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkconf)

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))


    val mapRDD: RDD[(String, Int)] = flatRdd.map(
      word =>{
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        (word,1)
      }
    )
      //RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，
     //而是触发后面的 action 算子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。
    mapRDD.cache()

    val reduceRdd: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRdd.collect().foreach(println)

    println("===============================================")


     //如果RDD需要重复使用  那么需要从头再次执行获取数据
    val groupRdd: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
      groupRdd.collect().foreach(println)

     sc.stop()
  }

}
