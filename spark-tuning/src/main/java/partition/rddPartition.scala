package partition

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.InitUtil



object rddPartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PartitionTuning")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")//为了演示效果，先禁用了广播join
      .set("spark.default.parallelism","16")
      .setMaster("local[*]")
    val session: SparkSession = InitUtil.initSparkSession(sparkConf)
    val df: DataFrame = session.sql("select * from sparktuning.salecourse_detail where courseid ='701' ")

    df.rdd.saveAsTextFile("/output")





    session.close()
  }

}
