package cbo

import utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CBOTuning {


  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("CBOTuning")
       .set("spark.sql.cbo.enabled", "true")//CBO 总开关。 true 表示打开，false 表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成。
        .set("spark.sql.cbo.joinReorder.enabled", "true")//使用 CBO 来自动调整连续的 inner join 的顺序。true：表示打开，false：表示关闭
       .set("spark.sql.cbo.joinReorder.dp.threshold", "12")//使用 CBO 来自动调整连续 inner join 的表的个数阈值。如果超出该阈值12，则不会调整 join 顺序。

         .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    val sqlstr =
      """
        |select
        |  csc.courseid,
        |  sum(cp.paymoney) as coursepay
        |from course_shopping_cart csc,course_pay cp
        |where csc.orderid=cp.orderid
        |and cp.orderid ='odid-0'
        |group by csc.courseid
      """.stripMargin

    sparkSession.sql("use sparktuning;")
    sparkSession.sql(sqlstr).show()

    while (true){}

  }
}
