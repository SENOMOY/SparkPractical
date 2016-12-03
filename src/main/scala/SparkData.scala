package com.spark.sample

import com.spark.sample.GetContext.{sparkContext, sqlContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/**
  * Created by senomoy on 28-08-2016.
  */
object SparkData {
  val seq = Seq(1, 2, 3, 2)
  val seqRDD = sparkContext.parallelize(seq)
  val arr = Array(1, 2, 4, 5, 6)
  val arrRDD = sparkContext.parallelize(arr,3)
  val list = List("one", "two", "three")
  val listRDD = sparkContext.makeRDD(list)
  val range = 0 to 100 by 7
  val rangeRDD = sparkContext.makeRDD(range)
  val string = Seq("1st one", "2nd two", "3rd three")
  val stringRDD = sparkContext.makeRDD(string)
  val emp = Seq(("A1", 1), ("B2", 2), ("C3", 3))
  val empRDD = sparkContext.makeRDD(emp)
  val std = Seq(("D1", 1), ("E2", 2), ("F3", 3))
  val stdRDD = sparkContext.makeRDD(std)
  val tuple = Seq((1,1),(1,2),(1,3))
  val tupleRDD = sparkContext.makeRDD(tuple)
  val rangeSQL = sqlContext.range(0,10)
  val fileInput = sparkContext.textFile("C:/file.txt")
  val hdfsFileInput = sparkContext.textFile("hdfs://file.txt")
  val tableSchema = StructType(Seq(StructField("COMPANY_ID", IntegerType, true),StructField("Company", StringType, true), StructField("Domain", StringType, true)))
}
