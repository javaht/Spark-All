package partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object rddPartition {
  def main(args: Array[String]): Unit = {


    val scf = new SparkConf().setMaster("local[*]").setAppName("teseet")
    val sc = new SparkContext(scf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxxx"),
      ("cba", "xxxxxxxxxx"),
      ("wnba", "xxxxxxxx"),
      ("wnba", "xxxxxxxxxx")
    ), 3)

    //自定义分区器  这个有意思  嘎嘎强 掌握
    val partRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartition)
    val valueRdd: RDD[(String, String)] = partRdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (0 == index) {
          //只保留第二个分区的数据
          iter
        } else {
          Nil.iterator //这个代表空的迭代器
        }
      }
    )

    valueRdd.collect().foreach(println)

    sc.stop()
  }

  /*
  * 自定义分区器
  *
  * */

  class MyPartition extends  Partitioner{
    //分区的数量
    override def numPartitions: Int = 3

     //根据数据的key值返回数据的分区索引(从0开始)
    override def getPartition(key: Any): Int = {
      key match {
        case  "nba" => 0
        case  "wnba" => 1
        case  _ => 2
      }

    }

  }


}
