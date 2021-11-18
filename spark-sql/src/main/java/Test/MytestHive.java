package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class MytestHive {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("MyHiveTest");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
       // List<dimArea> dimAreaList = spark.table("dw.dim_area").as(Encoders.bean(dimArea.class)).collectAsList();
        //spark.table("").where("").select();

        Dataset<Row> dataset = spark.table("gmall.dim_base_province").where("province_name is not null ");


        //dataset.write().mode(SaveMode.Append).jdbc(url, "ylrz_cal_person_summary_neo", connectionProperties);

/*

         dataset.select(dataset.col("province_name").cast(DataTypes.StringType)).write().format("jdbc")
                 .option("url", "jdbc:mysql://192.168.20.62:3306/gmall?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                 .option("dbtable","provice").option("user", "root").option("password", "123456").mode(SaveMode.Overwrite)
                 .save();
*/




        spark.stop();
    }
}
