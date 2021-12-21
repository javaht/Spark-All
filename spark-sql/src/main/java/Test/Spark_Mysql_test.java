package Test;

import Test.Bean.AC01Bean;
import Test.Bean.UserInfoBean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.List;

public class Spark_Mysql_test {
    public static void main(String[] args) {


         /*
         * 读取mysql数据到hive 一般需要加上配置 .config("spark.sql.parquet.writeLegacyFormat", true)
         * */

        SparkConf sparkConf = new SparkConf().set("spark.sql.warehouse.dir", "hdfs://192.168.2.240:8020/user/hive/warehouse");
        SparkSession spark = SparkSession.builder().config(sparkConf).appName("sparkMysqlTest").master("local[*]").enableHiveSupport().getOrCreate();

/*

        Dataset<Row> rowDataset = spark.read().format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/testByzht?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong")
                .option("user", "root").option("password", "123456").option("dbtable", "all_date").load();
*/
        Dataset<Row> aa10 = spark.table("ods.ods_aa10");

        final HashMap<String, String> codeMap = new HashMap<String, String>();
        List<Row> aa10List = aa10.collectAsList();


        //aa10List.forEach(row -> codeMap.put(row.getString(3)+"_"+row.getString(4),row.getString(5)));

        for (int i = 0; i < aa10List.size(); i++) {
            codeMap.put(aa10List.get(i).getString(3)+"_"+aa10List.get(i).getString(4),      aa10List.get(i).getString(5));
        }
        //(AAC183_484,三级飞行通信员)

        Dataset<AC01Bean> ac01 = spark.table("ods.ods_ac01").as(Encoders.bean(AC01Bean.class));


        Dataset<UserInfoBean> result = ac01.map(new MapFunction<AC01Bean, UserInfoBean>() {
            @Override
            public UserInfoBean call(AC01Bean row) throws Exception {
                UserInfoBean userInfoBean = new UserInfoBean();

                userInfoBean.setAac005_name(getKey("AAC005", row.getAac005(), codeMap));
                userInfoBean.setAac008_name(getKey("AAC008", row.getAac008(), codeMap));
                userInfoBean.setAac009_name(getKey("AAC009", row.getAac009(), codeMap));
                userInfoBean.setAac011_name(getKey("AAC011", row.getAac011(), codeMap));
                userInfoBean.setAac060_name(getKey("AAC060", row.getAac060(), codeMap));
                userInfoBean.setAac011_name(getKey("AAC011", row.getAac011(), codeMap));
                userInfoBean.setAac024_name(getKey("AAC024", row.getAac024(), codeMap));
                userInfoBean.setAac017_name(getKey("AAC017", row.getAac017(), codeMap));
                userInfoBean.setAac183_name(getKey("AAC183", row.getAac183(), codeMap));
                userInfoBean.setAac028_name(getKey("AAC028", row.getAac028(), codeMap));
                userInfoBean.setAac033_name(getKey("AAC033", row.getAac033(), codeMap));
                userInfoBean.setAac084_name(getKey("AAC084", row.getAac084(), codeMap));
                userInfoBean.setAac014_name(getKey("AAC014", row.getAac014(), codeMap));
                userInfoBean.setAac015_name(getKey("AAC015", row.getAac015(), codeMap));
                userInfoBean.setCac220_name(getKey("CAC220", row.getCac220(), codeMap));

                return userInfoBean;
            }
        }, Encoders.bean(UserInfoBean.class));


        result.where("aac028_name ='农民工' ").show();

        //这里一般不同saveAsTable   这就百度查查什么原因
      /*  rowDataset.select("xm").write().mode(SaveMode.Overwrite).insertInto("default.mytestxm");
        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true");


      rowDataset.select("xm").write().format("hive").mode(SaveMode.Overwrite).saveAsTable("default.mytestxm");
      */




        spark.stop();



    }


    //key AAA183
    //code 484
    // codeMap = (AAC183_484,三级飞行通信员)
    private static String getKey(String key,String code, HashMap<String, String> codeMap){

         //codeMap = (AAC183_484,三级飞行通信员) 所以传入key返回value
          return codeMap.get(key+"_"+code);
    }

 }
