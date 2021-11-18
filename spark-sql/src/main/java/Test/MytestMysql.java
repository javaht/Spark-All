package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.fusesource.leveldbjni.All;

import java.util.List;

public class MytestMysql {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("我的缓存表测试").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
/*

        List<Alldate> allDateList = spark.read().format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("dbtable", "all_date")
                .option("user", "root").option("password", "123456").load()
                .as(Encoders.bean(Alldate.class)).collectAsList();

        for (int i = 0; i < allDateList.size(); i++) {

            System.out.println(allDateList.get(i).getXm());
        }
*/
        Dataset<Row> dataset = spark.read().format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("dbtable", "(select * from all_date) m").option("user", "root").option("password", "123456").load();

        //.cast(DataTypes.IntegerType)
        List<Row> rowList = dataset.select(dataset.col("xm"), dataset.col("sfzh")).collectAsList();


        for (int i = 0; i < rowList.size(); i++) {
            System.out.println(rowList.get(i));
        }
        spark.stop();
    }
}
