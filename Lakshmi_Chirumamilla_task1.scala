package com.inf.homework
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Lakshmi_Chirumamilla_task1 {
  def main(args: Array[String]){
   val conf = new SparkConf().setAppName("Word Count").setMaster("local[1]")
   val sparkContext = new SparkContext(conf)
     if (args.length <2) {
       println("Please provide sufficient arguments from command line as per readMe")
       System.exit(1)
     }

    val usersDat = sparkContext.textFile(args(0))
    val  ratingsDat = sparkContext.textFile(args(1))
    

    
  val split_Users = usersDat.map(line=>line.split("\\::")).map(line=>(line(0).toInt, line(1).toString))
    val split_Ratings = ratingsDat.map(line=>line.split("\\::")).map(line=>(line(0).toInt, line(1).toInt,line(2).toDouble))   
    val user_items = split_Users.map(x=>(x._1,x))
    val rating_items = split_Ratings.map(x=>(x._1,x))
   
     var result = user_items.join(rating_items).map(t=>((t._2._2._2, t._2._1._2),(t._2._2._3))).mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).sortByKey().collect
 sparkContext.parallelize(result.map { t =>(""+t._1._1+","+t._1._2+","+t._2+"")}).saveAsTextFile("Lakshmi_Chirumamilla_result_task1.txt")

  
    sparkContext.stop()
  }
    
}