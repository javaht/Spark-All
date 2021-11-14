package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req_HotCategory2 {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req_HotCategory")
    val sc = new SparkContext(SparkConf)

    //1.读取原始日志数据

    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //原来方式存在的问题  actionRDD重复使用  cogroup性能可能较低
    actionRDD.cache()
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


    //(品类ID,(点击数量,0,0))
    //(品类ID,(点击数量,下单数量,0))
    //(品类ID,(点击数量,下单数量,支付数量))
    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }

    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRdd.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }


    rdd3.foreach(println)

     //将三个数据源合并在一起 统一进行聚合计算
     val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)






    val analisisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        //这里其实就是 sourceRDD 中 两两value的聚合
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    analisisRDD.foreach(println)

    val resultRDD: Array[(String, (Int, Int, Int))] = analisisRDD.sortBy(_._2, false).take(10)

    //resultRDD.foreach(println)





    sc.stop()

  }

}
