package AllSuanZi.transform.value.noshuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class mapPartitions_Java {
    public static void main(String[] args) {
        //mapPartitions可以以分区为单位进行数据转换操作
        //但是会将整个分区的数据架子啊到内存进行引用
         //如果处理完的数据是不会被释放掉的  存在对象的引用
        //在内存较小 但是数据量较大的情况下  容易引出内存溢出
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> datas = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(datas,2);

        JavaRDD<Integer> javaRDD = rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) integerIterator -> {
            System.out.println(">>>>>>>>>>");

            return integerIterator;
        });




        for (Integer integer : javaRDD.collect()) {

            System.out.println(integer);
        }

        sc.stop();
    }
}

