package com.spark.sample

import com.spark.sample.SparkData._
import com.spark.sample.GetContext.sparkContext
/**
  * Created by senomoy on 01-09-2016.
  */
object SparkBroadcast extends App{

  val bc1 = sparkContext.broadcast("Even: ")
  val bc2 = sparkContext.broadcast("Odd: ")
  rangeRDD.map(x=> {if(x!=0 && x%2==0) println(bc1.value+x) else println(bc2.value+x)}).collect
}
