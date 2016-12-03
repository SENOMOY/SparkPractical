package com.spark.sample

import com.spark.sample.GetContext.sparkContext
/**
  * Created by senomoy on 11-09-2016.
  */
object Transpose extends App{

  val rdd = sparkContext.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9)))
  rdd.foreach(println)
  val transposed = sparkContext.parallelize(rdd.collect.toSeq.transpose)
  transposed.foreach(println)
}
