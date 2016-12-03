package com.spark.sample

import com.spark.sample.GetContext.sqlContext
import org.apache.spark.sql.functions.{lead, lag}
import org.apache.spark.sql.expressions.Window
/**
  * Created by senomoy on 14-09-2016.
  */
case class EmpSalary(depName: String, empNo: Long, salary: Long)

object SparkLeadLag extends App {
  import sqlContext.implicits._
  val empsalary = Seq(
    EmpSalary("sales", 1, 5000),
    EmpSalary("personnel", 2, 3900),
    EmpSalary("sales", 3, 4800),
    EmpSalary("sales", 4, 4800),
    EmpSalary("personnel", 5, 3500),
    EmpSalary("develop", 7, 4200),
    EmpSalary("develop", 8, 6000),
    EmpSalary("develop", 9, 4500),
    EmpSalary("develop", 10, 5200),
    EmpSalary("develop", 11, 5200)).toDF

  val lagDep = lag('empNo, 1).over(Window.partitionBy('depName).orderBy('salary))
  empsalary.select('*, lagDep as 'lag).show

  val leadDep = lead('empNo, 1).over(Window.partitionBy('depName).orderBy('salary))
  empsalary.select('*, leadDep as 'lead).show

}
