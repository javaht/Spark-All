package com.zht

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object SaveToKafka {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("kafka").master("local[*]").enableHiveSupport().getOrCreate()


    val df: DataFrame = session.sql("select * from gmall.dwd_traffic_display_inc ")

    println("核的数量是：   " + df.rdd.getNumPartitions)
    df.cache()
    session.sparkContext.setCheckpointDir("ckps")
    df.checkpoint()


    val prop = new Properties()
    // 添加配置
    prop put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](prop)
    val record  =new ProducerRecord[String,String]("atguigu",)
    producer.send(record)



    session.close()
  }
}
