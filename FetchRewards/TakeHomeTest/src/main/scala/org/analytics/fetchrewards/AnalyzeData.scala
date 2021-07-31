package org.analytics.fetchrewards

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, floor, max, months_between, round, row_number, sum, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}

class AnalyzeData (sqlContext: SQLContext) {
  import sqlContext.implicits._
  def retrieveNMonthsData(noOfMonths: Int): DataFrame ={

    //Reading data from local disk. Update the path to read from project resources directory.
    val originalUserReceiptsDf = sqlContext.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_lat_v3.csv")

    val originalReceiptDescriptionDf = sqlContext.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_item_lat_v2.csv")

    //Get necessary information only
    val necessaryUserDataDF = originalUserReceiptsDf.select("RECEIPT_ID", "USER_ID", "STORE_NAME", "STORE_CITY", "STORE_STATE", "RECEIPT_PURCHASE_DATE", "RECEIPT_TOTAL", "RECEIPT_ITEM_COUNT")
    val necessaryReceiptDescDataDf = originalReceiptDescriptionDf.select("REWARDS_RECEIPT_ID", "RECEIPT_DESCRIPTION","QUANTITY", "ITEM_PRICE", "DISCOUNTED_PRICE", "BRAND", "CATEGORY")

    //Compute maximum date to find the date difference. Used to segregate data of specific months
    val maxDateWindow = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    //Get all the user and receipt information together for the analytics queries to run.
    //Note: All the data is not going to be needed by all the queries. Keep for adding different queries.
    //Fill null columns with 'Other' for naming
    val completeInfoDf = necessaryUserDataDF.
      join(necessaryReceiptDescDataDf, necessaryUserDataDF("RECEIPT_ID") === necessaryReceiptDescDataDf("REWARDS_RECEIPT_ID")).
      na.fill("Other", Seq("CATEGORY")).
      na.fill("Other", Seq("STORE_STATE")).
      withColumn("MAX_DATE", max("RECEIPT_PURCHASE_DATE").over(maxDateWindow)).
      withColumn("DATE_DIFF", round(months_between($"MAX_DATE", $"RECEIPT_PURCHASE_DATE"))).
      drop("REWARDS_RECEIPT_ID", "MAX_DATE")

    //UDF to get sub-category from category to in-depth analysis. Get the second category.
    //Example: Grocery|Canned & Packages => Canned & Packages
    // Other => Other
    val getSubCategory =  (strCategory:String) => {
      val arr = strCategory.split("\\|")
      if(arr.size >= 2)
        arr(1)
      else
        arr(0)
    }

    val getSubCategoryUDF = udf(getSubCategory)

    //Get n months of data specified in the argument
    val nMonthsDataDf = completeInfoDf.
      filter(completeInfoDf("DATE_DIFF") >= noOfMonths).
      withColumn("SUB_CATEGORY", getSubCategoryUDF('CATEGORY)).
      drop("DATE_DIFF", "CATEGORY")

    nMonthsDataDf
  }

  //Query 1 : Find best and least performing store by state
  def storePerformanceByState(nMonthsDataDf: DataFrame): (DataFrame, DataFrame) ={
    val byStoreStateDesc = Window.partitionBy("STORE_STATE").orderBy($"TOTAL_REVENUE".desc)
    val byStoreStateAsc = Window.partitionBy("STORE_STATE").orderBy($"TOTAL_REVENUE".asc)

    //Aggregate revenue for every store grouped by state to get best and least performing stores in each state
    val stateWiseStoreRevenue = nMonthsDataDf.
      groupBy($"STORE_STATE",$"STORE_NAME").
      agg(sum(bround($"ITEM_PRICE")).as("TOTAL_REVENUE"))

    //Get best performing stores by state
    val bestPerformingStoreByState = stateWiseStoreRevenue.
      withColumn("rowNum", row_number().over(byStoreStateDesc)).
      where("rowNum == 1").drop("rowNum").
      withColumnRenamed("STORE_NAME", "BEST_SELLING_STORE")

    //Get least performing stores by state
    val leastPerformingStoreByState = stateWiseStoreRevenue.
      withColumn("rowNum", row_number().over(byStoreStateAsc)).
      where("rowNum == 1").drop("rowNum").
      withColumnRenamed("STORE_NAME", "LEAST_SELLING_STORE")

    (bestPerformingStoreByState, leastPerformingStoreByState)
  }
}
