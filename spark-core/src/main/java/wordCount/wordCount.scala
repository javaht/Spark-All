package wordCount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object wordCount {
  def main(args: Array[String]): Unit = {
    //这里总结实现wordcount的算子的方式

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)

     wordCount1(sc)



    sc.stop()
  }

   //groupBy
  def wordCount1(sc : SparkContext): Unit={
    val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
    //下方会形成   Hello  scala  Hello  spark
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    //下方会形成
    //(scala,CompactBuffer(scala))
    //(spark,CompactBuffer(spark))
    //(Hello,CompactBuffer(Hello, Hello))
    val group: RDD[(String, Iterable[String])] = flatMapRdd.groupBy(word => word)

    val iterRdd: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    iterRdd.collect().foreach(println)


  }

  //groupByKey
  def wordCount2(sc : SparkContext): Unit={
    val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = word.groupByKey()
    val wordcount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //reduceBykey
  def wordCount3(sc : SparkContext): Unit={
    val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    val wordcount: RDD[(String, Int)] = word.reduceByKey(_ + _)
  }


  //aggregateByKey
  def wordCount4(sc : SparkContext): Unit={
    val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    val wordcount: RDD[(String, Int)] = word.aggregateByKey(0)(_ + _, _ + _)
  }


  def wordCount5(sc : SparkContext): Unit= {
    val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    val wordcount: RDD[(String, Int)] = word.foldByKey(0)(_ + _)

  }
    //combineByKey
    def wordCount6(sc: SparkContext): Unit = {
      val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
      val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
      val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
      val wordCount: RDD[(String, Int)] = word.combineByKey(
        v => v,
        (x: Int, y) => x + y,
        (x: Int, y: Int) => x + y
      )
    }


      //countByKey
      def wordCount7(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
        val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
        val word: RDD[(String, Int)] = flatMapRdd.map((_, 1))
        val countKey: collection.Map[String, Long] = word.countByKey()
      }

      //countByValue
      def wordCount8(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello scala", "Hello spark"))
        val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = flatMapRdd.countByValue()


      }






    }

