package com.spark.sample

import com.spark.sample.GetContext.sqlContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by senomoy on 14-09-2016.
  */
case class Salary(depName: String, empNo: Long, salary: Long)

object SparkWindow extends App{

  import sqlContext.implicits._
  val empsalary = Seq(
    Salary("sales", 1, 5000),
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("personnel", 5, 3500),
    Salary("develop", 7, 4200),
    Salary("develop", 8, 6000),
    Salary("develop", 9, 4500),
    Salary("develop", 10, 5200),
    Salary("develop", 11, 5200)).toDF

  val byDepName = Window.partitionBy('depName)
  val salaryAvg = avg('salary).over(byDepName)

  empsalary.select('*, salaryAvg as 'avg).show
}
