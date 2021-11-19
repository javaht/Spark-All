package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class MytestOutHive {
    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("MyHiveTest");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        Dataset<Row> dataset = spark.table("gmall.dim_base_province").where("province_name is not null ");




         dataset.select(dataset.col("province_name").cast(DataTypes.StringType)).write().format("jdbc")
                 .option("url", "jdbc:mysql://192.168.20.62:3306/gmall?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                 .option("dbtable","provice").option("user", "root").option("password", "123456").mode(SaveMode.Overwrite)
                 .save();

        spark.stop();
    }
}
