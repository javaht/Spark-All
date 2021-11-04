package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("从内存中创建RDD测试")
    val sparkContext = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))

    rdd.map((num: Int) => {
      num * 2
    })
    rdd.map((num: Int) => num * 2) //逻辑只有一行 花括号省略
    rdd.map((num) => num * 2) //类型自动推断  类型可以省略
    rdd.map(num => num * 2) //参数只有一个  小括号可以省略
    val makeRdd: RDD[Int] = rdd.map(_ * 2) //参数在逻辑中只出现一次 顺序出现 用_代替

    makeRdd.collect().foreach(println)


    sparkContext.stop()
  }

}
