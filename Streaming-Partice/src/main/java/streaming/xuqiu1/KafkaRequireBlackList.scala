package streaming.xuqiu1

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.JdbcUtil

import scala.collection.mutable.ListBuffer

/*
* 这个是队列  模拟的假数据
* */
object KafkaRequireBlackList {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDs = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))

    val adClickData: DStream[AdClickData] = kafkaDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    //判断点击用户是否在黑名单中
    // 如果用户不在黑名单中 那么进行统计数量(每个采集周期)
    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        //通过jdbc周期性的获取黑名单数据
        val blacklist = ListBuffer[String]()
        val conn: Connection = JdbcUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")
        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blacklist.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()

        //判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blacklist.contains(data.user)
          }
        )
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1) //(word,count)
          }
        ).reduceByKey(_ + _)

      }
    )




    ds.foreachRDD(
      rdd =>{
        //RDD提供了一个算子 可以有效提升效率  foreachPartition以分区创建连接对象
        rdd.foreachPartition(
          iter=>{
            val conn = JdbcUtil.getConnection
            iter.foreach{
              case ((day, user, ad), count) => {
                println(s"${day} ${user} ${ad} ${count}")
                if (count >= 30) {
                  //TODO 如果统计数量超过点击阈值(30) 那么将用户拉入到黑名单
                  val conn: Connection = JdbcUtil.getConnection
                  val sql =   """
                                |insert into black_list userid(userid) values(?)
                                |on duplicate key
                                |pdate userid = ?
                 """.stripMargin
                  JdbcUtil.executeUpdate(conn,sql,Array(user,user))
                  conn.close()
                } else {
                  //TODO 如果没有超过阈值那么需要将当天的广告点击数量进行更新
                  val conn: Connection = JdbcUtil.getConnection
                  val sql =               """
                                            |
                                            |select * from user_ad_count where
                                            |dt =? and  userid =? and adid=?
                  """.stripMargin
                  val flg: Boolean = JdbcUtil.isExist(conn, sql, Array(day, user, ad))
                  if(flg){
                    //查询统计表数据  如果存在数据  那么更新
                    val sql1 =       """
                                       |update user_ad_count
                                       |set count = count+?
                                       |where  dt =? and  userid =? and adid=?
                                       |
                    """.stripMargin
                    JdbcUtil.executeUpdate(conn,sql1,Array(count,day,user,ad))

                    //TODO 判断更新后的点击数据是否超过阈值 如果超过 那么将用户拉入到黑名单
                    val sql2 =  """
                                  |select *
                                  |from  user_ad_count
                                  |where  dt =? and  userid =? and adid=? and count>=30
                    """.stripMargin
                    val flg1: Boolean = JdbcUtil.isExist(conn, sql2, Array(day, user, ad))

                    if(flg1){
                      val sql3 =     """
                                       |insert into black_list userid(userid) values(?)
                                       |on duplicate key
                                       |pdate userid = ?
                 """.stripMargin
                      JdbcUtil.executeUpdate(conn,sql3,Array(user,user))
                    }

                  }else{
                    val sql4 = """
                                 |insert into user_ad_count(dt,userid,adid,count) values(?,?,?,?)
                    """.stripMargin
                    JdbcUtil.executeUpdate(conn,sql4,Array(day,user,ad,count))

                  }
                  conn.close()
                }

              }
            }


          }

    )
    ssc.start()
    ssc.awaitTermination()
  }
    )
  }

  //广告点击数据
  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)

}
