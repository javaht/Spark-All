package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req_HotCategory {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req_HotCategory")
    val sc = new SparkContext(SparkConf)

        //TODO1  Top10 热门品类

          //1.读取原始日志数据
          val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

          //2.统计品类的点击数量：(品类ID,点击数量)
          val clickActionRDD: RDD[String] = actionRDD.filter(
            action => {
              val datas: Array[String] = action.split("_")
              datas(6) != "-1"
            }
          )
        val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
          action => {
            val datas: Array[String] = action.split("_")
            (datas(6), 1)
          }
        ).reduceByKey(_ + _)


          //3.统计品类的下单数量  (品类ID,下单数量)
          val orderActionRDD = actionRDD.filter(
            action => {
              val datas: Array[String] = action.split("_")
              datas(8) !=  "null"
            }
          )
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(
          id => (id, 1)
        )

      }
    ).reduceByKey(_ + _)
          //4.统计支付的支付数量  (品类ID,支付数量)

    val payActionRDD = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) !=  "null"
      }
    )
    val payCountRdd: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("-")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    )



          //5.将品类进行排序 并且取前十名
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序：先比较第一个  再比较第二个 再比较第三个 以此类推
    //(品类ID,(点击数量,下单数量,支付数量))

/*    clickCountRDD.collect().foreach(println)
    println("*****************************************")
    orderCountRDD.collect().foreach(println)
    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")cv
    println("#################################################")*/
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRdd)

   // cogroupRdd.collect().foreach(println)


    //只对value进行操作
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRdd.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        var orderCnt = 0
        var iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }

        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }


    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)




          //6.将结果采集到控制台打印出来

    resultRDD.foreach(println)







    sc.stop()

  }

}
