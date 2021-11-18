package UDF;

import UDF.MyUdf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class SparkUdfUse {
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("我的缓存表测试").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();


        Dataset<Row> rowDataset = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("dbtable", "all_date").option("user", "root").option("password", "123456").load();



        rowDataset.createTempView("AllDate");

        spark.udf().register("myudf",new MyUdf(), DataTypes.StringType);

       spark.sql("select myudf(xm) from AllDate");


        spark.stop();
    }


}
