package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MytestOutHive {
    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("MyHiveTest");
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        spark.sql("show tables").show();



        spark.stop();
    }
}
