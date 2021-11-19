package Test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Spark_Mysql_test {
    public static void main(String[] args) {


         /*
         * 读取mysql数据到hive 一般需要加上配置 .config("spark.sql.parquet.writeLegacyFormat", true)
         * */

        SparkConf sparkConf = new SparkConf().set("spark.sql.warehouse.dir", "hdfs://192.168.2.240:8020/user/hive/warehouse");
        SparkSession spark = SparkSession.builder().config(sparkConf).appName("sparkMysqlTest").master("local[*]").enableHiveSupport().getOrCreate();


        Dataset<Row> rowDataset = spark.read().format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("user", "root").option("password", "123456").option("dbtable", "all_date").load();



         //这里一般不同saveAsTable   这就百度查查什么原因
      /*  rowDataset.select("xm").write().mode(SaveMode.Overwrite).insertInto("default.mytestxm");
        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true");

      */


        rowDataset.select("xm").write().format("hive").mode(SaveMode.Overwrite).saveAsTable("default.mytestxm");

        spark.stop();

    }
 }
