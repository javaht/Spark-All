package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class MytestHive {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "");
        SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName("MyHiveTest");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();


       // List<dimArea> dimAreaList = spark.table("dw.dim_area").as(Encoders.bean(dimArea.class)).collectAsList();
        //spark.table("").where("").select();

        Dataset<Row> dataset = spark.table("dw.dim_area").where("city_sk = '3710' ");


        //dataset.write().mode(SaveMode.Append).jdbc(url, "ylrz_cal_person_summary_neo", connectionProperties);

        dataset.select(dataset.col("area_sk").cast(DataTypes.StringType));




/*
        for (int i = 0; i < dimAreaList.size(); i++) {

            System.out.println(dimAreaList.get(i).getArea_sk());
        }
*/

        spark.stop();
    }
}
