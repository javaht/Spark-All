package transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Transform_java {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> datas = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> makeRdd = sc.parallelize(datas);
        JavaRDD<Object> mapRdd = makeRdd.map((Function<Integer, Object>) s -> {
             s =s *2 ;
            return s;
        });
        for (Object o : mapRdd.collect()) {
            System.out.println(o);
        }
        sc.stop();

    }
}
