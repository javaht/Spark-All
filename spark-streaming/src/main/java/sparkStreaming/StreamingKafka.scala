package sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingKafka {

  val sparkConf: SparkConf = new SparkConf().setAppName("我的streaming练习项目").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Durations.seconds(1))

  //3.定义 Kafka 参数
  val kafkaPara: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )


 val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent, //
    ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara)
  )


  kafkaDataDS.map(_.value()).print()




  ssc.start()
  ssc.awaitTermination()

}
