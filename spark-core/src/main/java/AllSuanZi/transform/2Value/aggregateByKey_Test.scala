package transform.transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey_Test {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //将数据根据不同的规则进行分区内计算和分区间计算。注意和reduceBykey(分区内和分区间的规则一样)比较



    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),("b",4),("b",5),("a",6)),2)
     //rdd结果的类型  取决于zeroValue的值
   // val aggRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)

   //获取相同数据的key的平均值
   val newRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
     (t, v) => {
       (t._1 + v, t._2 + 1)
     },
     (t1, t2) => {
       (t1._1 + t2._1, t1._2 + t2._2)
     }
   )

      //key保持不变  只对v进行转换
      val resultRdd: RDD[(String, Int)] = newRdd.mapValues {
        case (num, cnt) => {
          num / cnt
        }
      }


    println("这个是分区：  "+resultRdd.partitions.size)


    resultRdd.collect().foreach(println)

    sc.stop()
  }

}