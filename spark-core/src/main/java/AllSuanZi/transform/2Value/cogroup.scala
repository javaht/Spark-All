package transform.transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cogroup {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    // cogroup = connect + group
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",3),("a",2),("c",3)))
    val coreRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = dataRDD1.cogroup(dataRDD2)


    /*  这个是结果
     (a,(CompactBuffer(1),CompactBuffer(3, 2)))
    (b,(CompactBuffer(2),CompactBuffer()))
    (c,(CompactBuffer(3),CompactBuffer(3)))
*/

    coreRdd.collect().foreach(println)




    sc.stop()


  }

}
