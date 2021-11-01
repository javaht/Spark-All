package transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;


public class TranSform_txt_java {
    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("data/1.txt");
        JavaRDD<String> mapRdd = lines.map((Function<String, String>) s -> {
            String[] s1 = s.split(" ");
            return s1[2];
        });
        for (String s : mapRdd.collect()) {
            System.out.println(s);
        }


        sc.stop();
    }
}
