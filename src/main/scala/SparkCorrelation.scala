package com.spark.sample

import com.spark.sample.GetContext.sparkContext
import org.apache.spark.mllib.stat.Statistics
/**
  * Created by senomoy on 14-09-2016.
  */
object SparkCorrelation extends App {

  val firstRDD = sparkContext.makeRDD(Seq(1.1,2.2,3.3))
  val secondRDD = sparkContext.makeRDD(Seq(1.1,2.2,3.3))
  val corr1 = Statistics.corr(firstRDD, secondRDD, "spearman")
  val corr2 = Statistics.corr(firstRDD, secondRDD, "pearson")
}
