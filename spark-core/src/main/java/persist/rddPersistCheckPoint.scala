package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object rddPersistCheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkconf)

    //这里设置checkpoint的位置
    sc.setCheckpointDir("datas")

    val list = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRdd.map(
      word =>{
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        (word,1)
      }
    )
    //checkpoint需要落盘  需要指定检查点保存路径
    //检查点路径中保存的文件 当作业执行完毕后不会被删除
    //   一般的保存路径都是在分布式文件系统中
    mapRDD.cache()
  mapRDD.checkpoint()//先cache 再checkpoint  表示从缓存中取数据 不用从头走



    val reduceRdd: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //持久化操作必须在行动算子执行时完成
    reduceRdd.collect().foreach(println)

    println("===============================================")


     //如果RDD需要重复使用  那么需要从头再次执行获取数据
    val groupRdd: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRdd.collect().foreach(println)

     sc.stop()
  }

}
