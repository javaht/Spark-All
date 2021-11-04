package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Require {
  //案例实操

  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)
    //1.获取原始数据 时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

    val dataRdd: RDD[String] = sc.textFile("datas/agent.log")

    //2.将原始数据进行结构的转换。时间戳，省份，城市，用户，广告 =>   ((省份,广告),1)
    val mapRdd: RDD[((String, String), Int)] = dataRdd.map(
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    )

     //3.将转换结构后的数据进行分组聚合     ((省份,广告),1) =>   ((省份,广告),sum)
     val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)


    //4.将聚合的结果进行结构的转换     ((省份,广告),sum) =>    (省份,(广告,sum))
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }

    //5.将转换架构后的数据根据省份进行分组      (省份,(广告A,sumA),(广告B,sumB),(广告c,sumc))
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()

    //6. 将分组后的数据组内排序(根据数量降序取前三名)
     //key保持不变  只对v进行操作
     val resultRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
       iter => {
         iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3) //按照第二个参数排序
       }
     )


    //7.采集数据 打印在控制台
    resultRdd.collect().foreach(println)




    sc.stop()
  }

}
