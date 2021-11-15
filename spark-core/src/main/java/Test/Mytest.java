package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


import java.util.List;

public class Mytest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("我的缓存表测试").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();



        List<Alldate> allDateList = spark.read().format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("dbtable", "all_date").option("user", "root").option("password", "123456").load()
                .as(Encoders.bean(Alldate.class)).collectAsList();

        for (int i = 0; i < allDateList.size(); i++) {

            System.out.println(allDateList.get(i));
        }


        spark.stop();


    }
}

