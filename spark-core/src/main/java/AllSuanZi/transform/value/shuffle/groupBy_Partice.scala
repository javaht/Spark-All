package AllSuanZi.transform.value.shuffle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object groupBy_Partice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test222")
    val sc = new SparkContext(sparkConf)


    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val timeRdd: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1) //表示这个时间点 出现了一次
      }
    ).groupBy(_._1)
    //groupBy(_._1) 首先 _代表其中的元素   _1代表元素中的第一个参数 也就是说按照第一个元素hour分组  返回值是(hour,1)

    /*     val tuple: (Int, String, Boolean) = (40,"bobo",true)
     //（2）访问元组
     //（2.1）通过元素的顺序进行访问，调用方式：_顺序号
     println(tuple._1)
     println(tuple._2)
     println(tuple._3)*/

    timeRdd.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)


    sc.stop()

  }

}
