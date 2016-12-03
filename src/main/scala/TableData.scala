package com.spark.sample

import com.spark.sample.GetContext.{sparkContext, sqlContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
/**
  * Created by senomoy on 13-09-2016.
  */
object TableData {
  def tableData(tablename: String) = {
    tablename match {
      case "company_ranking" => {
        val tableSchema = StructType(Seq(StructField("ID", IntegerType, true),StructField("Company_Name", StringType, true), StructField("Domain", StringType, true), StructField("Rank", IntegerType, true), StructField("Established", StringType, true)))
        val tableData = Seq(Row(1, "IBM", "IT", 3, "1911"), Row(2, "Apple", "IT", 3, "1970"), Row(3, "Microsoft", "IT", 4, "1980"), Row(4, "Google", "IT", 1, "1970"))
        val tableRDD = sparkContext.makeRDD(tableData)
        sqlContext.createDataFrame(tableRDD, tableSchema).registerTempTable("company_ranking")
        sqlContext.sql("select * from company_ranking")
      }
      case "company_valuation" => {
        val tableSchema = StructType(Seq(StructField("ID", IntegerType, true), StructField("Share_Value", DoubleType, true), StructField("Turnover", StringType, true)))
        val tableData = Seq(Row(1, 124.05, "102B USD"), Row(2, 98.75, "90B USD"), Row(3, 78.80, "84.5B USD"))
        val tableRDD = sparkContext.makeRDD(tableData)
        sqlContext.createDataFrame(tableRDD, tableSchema).registerTempTable("company_valuation")
        sqlContext.sql("select * from company_valuation")
      }
      case "sample_data" => {
        val tableSchema = StructType(Seq(StructField("ID", IntegerType, true), StructField("Double_Value", DoubleType, true)))
        val tableData = Seq(Row(1, 124.05),Row(2, 124.05),Row(3,87.50),Row(4, 78.80),Row(5, 124.05),Row(3, 71.30),Row(6, 124.05),Row(7, 87.50),Row(8, 87.50))
        val tableRDD = sparkContext.makeRDD(tableData)
        sqlContext.createDataFrame(tableRDD, tableSchema).registerTempTable("sample_data")
        sqlContext.sql("select * from sample_data")
      }
    }
  }
}
