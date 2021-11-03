package transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

<<<<<<< HEAD
       //这个是根据相同的key 进行分组
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3),("d",4)),4)

    val reduceRdd: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    println("这个是分区：  "+reduceRdd.partitions.size)

    reduceRdd.collect().foreach(println)
=======
        //相同的key的数据 进行value数据的聚合操作
     //scala语言中 一般的聚合操作都是两两聚合 spark的聚合也是
    val dataRDD1 = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),4)
    val reduceRdd: RDD[(String, Int)] = dataRDD1.reduceByKey(_ + _)

    val reduceRdd2 = dataRDD1.reduceByKey(_+_, 1)

    reduceRdd2.collect().foreach(println)
    println("partitions: "+reduceRdd2.partitions.size)
>>>>>>> origin/master


    sc.stop()
  }

<<<<<<< HEAD
}
=======
}
>>>>>>> origin/master
