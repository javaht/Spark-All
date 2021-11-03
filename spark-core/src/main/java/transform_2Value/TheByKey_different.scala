package transform_2Value

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TheByKey_different {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),("b",4),("b",5),("a",6)),2)

   // rdd.groupByKey()


    rdd.reduceByKey(_+_)           //wordcount


    rdd.aggregateByKey(0)(_+_, _+_)        //wordcount


    rdd.foldByKey(0)(_+_)        //wordcount


    rdd.combineByKey(v=>v,(x:Int,y)=> x+y,(x:Int,y:Int)=>x+y)           //wordcount



    sc.stop()


  }

}
