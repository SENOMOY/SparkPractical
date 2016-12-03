package com.spark.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext,Milliseconds}

/**
  * Created by senomoy on 27-08-2016.
  */
object GetContext {
  val master = "local[2]"
  val appName = "SparkProgram"
  val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").setMaster(master).setAppName(appName)
  val sparkContext =  new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)
  val streamingContext = new StreamingContext(conf, Seconds(1))
}
