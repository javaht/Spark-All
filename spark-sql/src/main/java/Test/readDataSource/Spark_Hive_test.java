package Test.readDataSource;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Spark_Hive_test {
    public static void main(String[] args) {

        //读取hive的表存储到mysql上
        SparkConf sparkConf = new SparkConf().setAppName("我的缓存表测试").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();


        spark.table("dw.dim_area")
                .write().format("jdbc")
                .option("url","jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("driver","com.mysql.cj.jdbc.Driver").option("user","root").option("password","123456").option("dbtable","begging").save();

        spark.stop();
    }
}
