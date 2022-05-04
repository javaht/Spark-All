package map

import utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object MapSmallFileTuning {


  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("MapSmallFileTuning")
      .set("spark.files.openCostInBytes", "4194304") //默认4m  设置为接近小文件的大小是最合适的
      .set("spark.sql.files.maxPartitionBytes", "128MB") //默认128M
      .setMaster("local[1]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    sparkSession.sql("select * from sparktuning.course_shopping_cart")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.test")


    while (true) {}
  }
}
