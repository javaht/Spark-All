package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object diyDateSource {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("这是我的diy数据源").setMaster("local[*]");

    val ssc = new StreamingContext(sparkConf, Seconds(3));


    val messageDs: ReceiverInputDStream[String] = ssc.receiverStream(new Myreceiver)



    messageDs.print()

    ssc.start()



    ssc.awaitTermination()


   }


   /*
   * 自定义数据采集器
   * 1.继承
   *
   *
   * */
  class Myreceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
     private var flag = true

     override def onStart(): Unit = {

        new Thread(new Runnable {
          override def run(): Unit = {
            while(flag){
              val message ="采集的数据为： "+new Random().nextInt(10).toString
              store(message)
              Thread.sleep(500)
            }

          }
        }).start()

     }

     override def onStop(): Unit = {

         flag =false

     }
   }
}
