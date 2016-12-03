package com.spark.sample

import org.apache.spark.rdd.RDD
import com.spark.sample.GetContext.sparkContext
/**
  * Created by senomoy on 11-09-2016.
  */
object Median {
  def main(args: Array[String]): Unit = {
    val intRdd = sparkContext.makeRDD(1.0 to 20.5 by 1.5)
    intRdd.sortBy(c => c, false).zipWithIndex.foreach(println)
    val sorted = intRdd.sortBy(c => c, false).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }
    sorted.foreach(println)
    val count = sorted.count()
    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
    } else sorted.lookup(count / 2).head.toDouble
    println(median)
  }
}
