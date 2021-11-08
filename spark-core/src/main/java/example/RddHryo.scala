package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

 //kryo序列化
object RddHryo {
  def main(args: Array[String]): Unit = {
       //spark内部已经默认支持  自己使用的话可以像下方这样用
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyTest")
                                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                     .registerKryoClasses(Array(classOf[Searcher]))




    val sc = new SparkContext(SparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD2(rdd)
    result.collect.foreach(println)

    //RDD算子中传递的函数是会包含闭包操作 那么就会进行检测功能


    sc.stop()
  }

   //类的构造参数其实就是类的属性  构造参数需要进行闭包检测 其实就等同于类进行闭包检测
   class Searcher(val query: String)  {
    def isMatch(s: String) = {
      s.contains(this.query)
    }

     //RDD算子中传递的函数是会包含闭包操作 那么就会进行检测功能
     //函数序列化
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch)   //外部声明函数
    }

     //属性序列化
    def getMatchedRDD2(rdd: RDD[String]) = {
      val q = query
      rdd.filter(_.contains(q))
    }


  }



}
