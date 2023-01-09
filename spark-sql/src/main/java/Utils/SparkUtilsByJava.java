package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

    public class SparkUtilsByJava {
        public static SparkSession createSparkSession(String master, String partitions) throws IOException {
            initKerberos();
            master= "local[*]";
            System.setProperty("HADOOP_USER_NAME", "root");
            return SparkSession.builder()
                    .appName("")
                    .master(master)
                    .config("spark.sql.catalogImplementation", "hive")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                    .config("spark.jars.package", "org.apache.hudi:hudi-spark3-bundle_2.12:0.11.1")
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                    .config("spark.sql.shuffle.partitions", partitions)
                    .enableHiveSupport()
                    .getOrCreate();
        }


        public static void initKerberos() throws IOException {
            //val path = this.getClass.getClassLoader.getResource("").getPath
            String path = "D:\\soft\\IdeaWorkSpace\\hive_on_hudi\\src\\main\\resources\\";
            Configuration conf = new Configuration();
            System.setProperty("java.security.krb5.conf", path + "krb5.conf");
            conf.addResource(new Path(path + "hdfs-site.xml"));
            conf.addResource(new Path(path + "hive-site.xml"));
            conf.set("hadoop.security.authentication", "Kerberos");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("dfs.client.use.datanode.hostname", "true");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("ranger_all_public/hudi01.zxzh.com@ZXZH.COM", path + "ranger_all_public.keytab");
            System.out.println("login user: "+UserGroupInformation.getLoginUser());
        }

        public static Dataset odsProcess(Dataset ds, SparkSession session){
            // 获取第一行数据
            ds.first();
            Date date = new Date();
            SimpleDateFormat partitionpath_format = new SimpleDateFormat("yyyy");
            SimpleDateFormat data_createtime_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String partitionpath_str = partitionpath_format.format(date);
            String data_createtime_str = data_createtime_format.format(date);

            Dataset df = ds.withColumn("uuid_mid", functions.row_number().over(Window.orderBy("area_name")))
                    .withColumn("uuid", functions.col("uuid_mid"))
                    .withColumn("data_createtime",functions.lit(data_createtime_str))
                    .withColumn("data_updatetime", functions.lit(data_createtime_str))   // 数据最后更新时间
                    .withColumn("status", functions.lit(1))
                    .withColumn("ts", functions.lit(date.getTime())) // 添加timestamp列，作为Hudi表记录数据与合并时字段，使用发车时间
                    .withColumn("partitionpath",functions.lit(partitionpath_str)) // hudi 指定的分区字段
                    .drop("uuid_mid");

            return df;
        }


        public static void main(String[] args) throws IOException {

            SparkSession session = createSparkSession("","8");
            Dataset<Row> ds = session.sql("select area_code,area_name from hudi_ods.dict_area_number ");
            ds.show();
            odsProcess(ds,session).show();

            session.close();

        }


    }
