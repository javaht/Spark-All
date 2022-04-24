package AllSuanZi.transform.twoValue.ByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combineByKey {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //分区内和分区间的计算规则一样  就用foldByKey  相当于是aggregateBykey的特殊情况
    //
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


    //这个算子有三个参数
    //第一个参数：将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数：分区内的计算规则
    // 第三个参数：分区间的计算规则
    val comRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )


    //key保持不变  只对v进行转换
    val resultRdd: RDD[(String, Int)] = comRdd.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }

    println("这个是分区：  " + resultRdd.partitions.size)


    resultRdd.collect().foreach(println)

    sc.stop()
  }

}
