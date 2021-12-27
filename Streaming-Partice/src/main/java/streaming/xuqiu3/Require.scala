package streaming.xuqiu3

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.JdbcUtil

import scala.collection.mutable.ListBuffer

/*
* 这个是队列  模拟的假数据
* */
object Require {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDs = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara)
    )


    val adClickData = kafkaDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //最近一分钟 每10秒算一次
    //涉及窗口计算

    val reduceDs: DStream[(Long, Int)] = adClickData.map(
      data => {
        val ts: Long = data.ts.toLong
        val newts = ts / 10000 * 10000
        (newts, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(60), Seconds(10))
    reduceDs.foreachRDD(
      rdd =>{
         val list = ListBuffer[String]()
        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
       datas.foreach{
         case(time,count) =>{

           val timeString: String = new SimpleDateFormat("mm:ss").format(new java.util.Date(time.toLong))
           list.append(s"""{"xtime":"${timeString}", "yval":"${count}"}""")
         }
       }

        //输出文件
        val out =new PrintWriter(new FileWriter(new File("D:\\soft\\IDEA\\workplace\\Spark-All\\datas\\adclick\\adclick.json")))
        out.println("["+list.mkString(",")+"]")
        out.flush()
        out.close()
      }
    )



    ssc.start()
    ssc.awaitTermination()
  }
  //广告点击数据
  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)

}
