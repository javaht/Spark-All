package transform.transform_2Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object leftOuterJoin {
  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
    val sc = new SparkContext(SparkConf)


    //类似于 SQL 语句的左外连接
    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",3),("a",5),("d",3)))
    val rdd: RDD[(String, (Int, Option[Int]))] = dataRDD1.leftOuterJoin(dataRDD2)

    rdd.collect().foreach(println)


/*   这个是结果
    (a,(1,Some(3)))
    (a,(1,Some(5)))
    (b,(2,None))
    (c,(3,None))
*/

    sc.stop()


  }

}
