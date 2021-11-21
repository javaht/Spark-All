package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamCount {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("我的stream测试")
    val ssc = new StreamingContext(sparkConf, Seconds(4))


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999);


    val words: DStream[String] = lines.flatMap(_.split(" "))


    val wordsToOne = words.map((_, 1))

    val wordCount = wordsToOne.reduceByKey(_ + _)

    wordCount.print()

    ssc.start()


    ssc.awaitTermination()

   // ssc.stop()
  }

}
