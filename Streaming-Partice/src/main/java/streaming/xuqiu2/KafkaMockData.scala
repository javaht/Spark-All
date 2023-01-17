package streaming.xuqiu2

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object KafkaMockData {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者
    val producer = new KafkaProducer[String, String](prop)
    while(true) {
      mockData().foreach(
        data =>{
          //println(data)
          //向Kakfa中生成数据
          val record  =new ProducerRecord[String,String]("atguigu",data)
          producer.send(record)
        }
      )
      Thread.sleep(2000)

    }

    def mockData()  ={
      val list = ListBuffer[String]()
      var arealist  = ListBuffer[String]("华北","华东","华南")
      var citylist  = ListBuffer[String]("北京","上海","深圳")

      for( i<- 1 to 30){

        val  area  = arealist(new Random().nextInt(3))
        val city  =  citylist(new Random().nextInt(3))
        var userid  = new Random().nextInt(6)+1
        var adid = new Random().nextInt(6)+1
        list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
      }
      list
    }
  }




}
