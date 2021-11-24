package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class wordCount {

    public static void main(String[] args) {

        //创建环境对象

        SparkConf sparkConf = new SparkConf().setAppName("我的streaming练习项目").setMaster("local[*]");
        //StreamingContext 需要传递两个参数 一个参数表示环境配置.第二个参数表示批处理的周期(采集周期)
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));




        //获取端口数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        JavaDStream<String> stringJavaDStream = lines.flatMap(row -> Arrays.asList(row.split(" ")).iterator());






        //逻辑处理







        //关闭环境

        ssc.stop(true);

    }
}
