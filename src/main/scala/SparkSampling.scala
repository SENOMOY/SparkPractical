package com.spark.sample

import com.spark.sample.GetContext.sparkContext
import com.spark.sample.SparkData._
/**
  * Created by senomoy on 14-09-2016.
  */
object SparkSampling extends App {

  val data = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
  val sampledData = data.sample(false,0.5,3)
  val sampledData1 = data.takeSample(false,2,1)
  val sampleDataSize = sampledData.count()
  val rawDataSize = seqRDD.count()
  sampledData.foreach(println)
  println("******************")
  sampledData1.foreach(println)
 // println(rawDataSize + " and after the sampling: " + sampleDataSize)

}
