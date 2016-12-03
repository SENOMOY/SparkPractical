package com.spark.sample

import com.spark.sample.SparkData._
import org.apache.spark

/**
  * Created by senomoy on 28-08-2016.
  */
object PairRDD {
  def main(args: Array[String]): Unit = {
    println("******Pair RDD*******")
    val pairRDD = stringRDD.map(s => (s.split(" ")(0), s))
    pairRDD.foreach(println)
    println("******RDD partitioner*******")
    println(pairRDD.partitioner)
    val partioned = pairRDD.partitionBy(new spark.HashPartitioner(2))
    println(partioned.partitioner)
    println("******RDD mapValues*******")
    tupleRDD.mapValues(_ + 1).foreach(println)
    println("******RDD flatMapValues*******")
    tupleRDD.flatMapValues(x => (x to 5))
    println("******RDD keys*******")
    tupleRDD.keys.foreach(println)
    println("******RDD values*******")
    tupleRDD.values.foreach(println)
    println("******RDD reduceByKey*******")
    tupleRDD.reduceByKey((x, y) => x + y)
    println("******RDD groupByKey*******")
    tupleRDD.groupByKey().foreach(println)
    println("******RDD sortByKey*******")
    tupleRDD.sortByKey().foreach(println)
    println("******RDD join*******")
    tupleRDD.join(tupleRDD).foreach(println)
  }
}
