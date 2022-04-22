package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MytestOutHive {
    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("MyHiveTest");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        Dataset<Row> dataset = spark.table("ods.ods_aa10").where("aaa100 = 'AAC005' ");


         dataset.select(dataset.col("aaa103")).write().format("jdbc")
                 .option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                 .option("driver","com.mysql.cj.jdbc.Driver")
                 .option("dbtable","Myaa10").option("user", "root").option("password", "123456").mode(SaveMode.Overwrite).save();

        spark.stop();
    }
}
