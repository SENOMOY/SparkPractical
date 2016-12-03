package com.spark.sample

import com.spark.sample.SparkData._
import org.apache.spark.storage.StorageLevel

/**
  * Created by senomoy on 27-08-2016.
  */
object RDDOperation {

  def main(args: Array[String]): Unit = {

    println("*****Seq RDD with filter*****")
    val newRDD1 = seqRDD.filter(_ > 1)
    newRDD1.foreach(println)
    println("*****Array RDD with filter*****")
    val newRDD2 = arrRDD.filter(_ > 4)
    newRDD2.foreach(println)
    println("*****RDD union*****")
    val unionRDD = newRDD1.union(newRDD2)
    unionRDD.foreach(println)
    println("*****RDD join*****")
    val joinedRDD = empRDD.join(stdRDD)
    println(joinedRDD.count)
    println("*****RDD count*****")
    println(unionRDD.count)
    println("*****RDD countByValue*****")
    println(seqRDD.countByValue)
    println("*****RDD take*****")
    unionRDD.take(1).foreach(println)
    println("*****RDD top*****")
    unionRDD.top(1).foreach(println)
    println("*****RDD map*****")
    val mapRDD = unionRDD.map(_ * 2)
    mapRDD.foreach(println)
    println("*****RDD flatMap*****")
    val newRDD3 = listRDD.flatMap(x => x)
    newRDD3.foreach(x => print(x + ","))
    val newRDD4 = stringRDD.flatMap(s => s.split(" "))
    newRDD4.foreach(println)
    println("*****RDD distinct*****")
    val newRDD5 = seqRDD.distinct()
    newRDD5.foreach(println)
    println("*****RDD substract*****")
    val newRDD6 = arrRDD.subtract(seqRDD)
    newRDD6.foreach(println)
    println("*****RDD intersection*****")
    val newRDD7 = arrRDD.intersection(seqRDD)
    newRDD7.foreach(println)
    println("*****RDD cartesian*****")
    val newRDD8 = listRDD.cartesian(stringRDD)
    newRDD8.foreach(println)
    println("*****RDD reduce*****")
    val intVal = seqRDD.reduce((a, b) => a + b)
    println(intVal)
    println("Seq RDD partions number: " + seqRDD.partitions.length)
    println("*****RDD fold*****")
    //empRDD.fold(stdRDD)
    println("*****RDD persist*****")
  //  empRDD.persist
   // empRDD.persist(StorageLevel.DISK_ONLY)
    // StorageLevel.MEMORY_AND_DISK;StorageLevel.MEMORY_ONLY
  //  println("*****RDD cache*****")
    //empRDD.cache

  }
}
