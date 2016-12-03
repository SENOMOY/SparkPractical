package com.spark.sample
import com.spark.sample.SparkData._
/**
  * Created by senomoy on 28-08-2016.
  */
object WordCount extends  App{
  val words = fileInput.flatMap(x => x.split(","))
  val result1 = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
  //result1.foreach(print)
  //println()
  val result2 = words.countByValue
 // result2.foreach(println)
  result1.saveAsTextFile("D:/sp/")
}
