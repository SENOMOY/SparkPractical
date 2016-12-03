package com.spark.sample

import com.spark.sample.SparkData._
import com.spark.sample.TableData._
import org.apache.spark.sql.functions._
import com.spark.sample.GetContext.{sparkContext, sqlContext}
/**
  * Created by senomoy on 12-09-2016.
  */
object ScalaDF extends App {
 // rangeSQL.show
 // rangeSQL.orderBy(desc("id")).show
 // rangeSQL.repartition(3).show
 // empRDD.sample(false,0.1,1234).foreach(println)
 //.where("Established = 1980")
 //sqlContext.cacheTable("company_ranking")
 //sqlContext.cacheTable("company_valuation")
 val newCol = udf((sval: Double) =>
  if(sval > 100.0) "Up" else "Down"
 )
  val tbl1 = tableData("company_ranking")
  tbl1.schema.fields.foreach(f=>
    println(f.name +" : "+ f.dataType)
  )
  tbl1.show
  val tbl2 = tableData("company_valuation")
  tbl2.schema.fields.foreach(f=>
    println(f.name +" : "+ f.dataType)
  )
  tbl2.show
  val tbl3 = tableData("sample_data")
  tbl3.show
 tbl2.withColumnRenamed("Turnover","Gross_Input").show
 tbl2.withColumn("Reputation",newCol(tbl2("Share_Value"))).show

  //tbl3.select("Double_Value").distinct().show
  //tbl3.describe().show
  // `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`
  tbl1.join(tbl2,tbl2("ID") === tbl1("ID"),"left_outer").show
  //tbl.describe().show
  tbl1.select("Domain").distinct().show
  //tbl.where("Rank >= 3").show
  //sqlContext.uncacheTable("company_ranking")
  //sqlContext.uncacheTable("company_valuation")
}