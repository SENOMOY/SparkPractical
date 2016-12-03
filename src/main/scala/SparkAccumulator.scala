package com.spark.sample

import com.spark.sample.SparkData._
import com.spark.sample.GetContext.sparkContext
/**
  * Created by senomoy on 31-08-2016.
  */
object SparkAccumulator extends App{
  val ac = sparkContext.accumulator(0)
  rangeRDD.foreach(println)
  println("********")
  rangeRDD.foreach(x=> {if(x%14==0)ac+=1})
  println(ac.value)

}
