package org.analytics.fetchrewards

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, countDistinct, desc, floor, max, months_between, round, row_number, sum, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}

class AnalyzeData (sqlContext: SQLContext, sparkSession: SparkSession) {
  import sqlContext.implicits._
  def retrieveNMonthsData(noOfMonths: Int, userDataInputPath: String, receiptDescriptionInputPath: String): DataFrame ={

    /*Not able to read inputs from resources while running jar. Recheck later.
    val userReceiptInputFile = getClass.getResource("/rewards_receipts_lat_v3.csv")
    val receiptDescriptionInputFile = getClass.getResource("/rewards_receipts_item_lat_v2.csv")
    println(userReceiptInputFile.getFile)
    println(receiptDescriptionInputFile.getFile)*/

    //Reading data from local disk. Update the path to read from project resources directory.
    val originalUserReceiptsDf = sparkSession.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(userDataInputPath)
      //load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_lat_v3.csv")

    val originalReceiptDescriptionDf = sparkSession.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(receiptDescriptionInputPath)
      //load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_item_lat_v2.csv")

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

    //UDF to get sub-category from category for in-depth analysis. Get the second category.
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
    //Use maximum date available in the data as start date, and get n months of data prior to that date
    val nMonthsDataDf = completeInfoDf.
      filter(completeInfoDf("DATE_DIFF") <= noOfMonths).
      withColumn("SUB_CATEGORY", getSubCategoryUDF('CATEGORY)).
      drop("DATE_DIFF", "CATEGORY")

    nMonthsDataDf
  }

  //Query 1 : Find best and least performing store (as per revenue) by state
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

  //Query 2 : Find % contribution of top 5 selling sub categories for each store
  def bestSelling5CategoriesByStore(nMonthsDataDf: DataFrame): DataFrame ={
    val byStoreNameAndSubCategory = Window.partitionBy("STORE_NAME").orderBy($"INDIVIDUAL_ITEM_TOTAL_SALE_DISCOUNTED_PRICE".desc)

    //Get total sales per sub-category for each store
    val totalSalesPerSubCategoryDf = nMonthsDataDf.
      groupBy($"STORE_NAME",$"SUB_CATEGORY").
      agg(sum(bround($"ITEM_PRICE")).
        as("INDIVIDUAL_ITEM_TOTAL_PRICE"), sum(bround($"DISCOUNTED_PRICE")).
        as("INDIVIDUAL_ITEM_TOTAL_SALE_DISCOUNTED_PRICE"))

    //Get revenue wise top 5 sub-categories for each store
    val top5SubCategoriesPerStore = totalSalesPerSubCategoryDf.
      withColumn("rowNum", row_number().over(byStoreNameAndSubCategory)).
      where("rowNum < 6").drop("rowNum")

    //Get total revenue for each store
    //Total revenue is redundant data for next step, but keep for convenience of reading
    val storeWiseTotalRevenueDf = nMonthsDataDf.
      groupBy(col("STORE_NAME").as("STORENAME")).
      agg(sum(bround($"DISCOUNTED_PRICE")).
        as("TOTAL_REVENUE"))

    //Calculate % of contribution for top 5 sub-categories for each store
    val top5SubCategoryTotalRevContri = top5SubCategoriesPerStore.
      join(storeWiseTotalRevenueDf, top5SubCategoriesPerStore("STORE_NAME").equalTo(storeWiseTotalRevenueDf("STORENAME"))).
      withColumn("REVENUE_CONTRIBUTION_BY_%", floor(top5SubCategoriesPerStore("INDIVIDUAL_ITEM_TOTAL_SALE_DISCOUNTED_PRICE").multiply(100).divide(storeWiseTotalRevenueDf("TOTAL_REVENUE")))).
      drop(storeWiseTotalRevenueDf("STORENAME")).
      drop(top5SubCategoriesPerStore("INDIVIDUAL_ITEM_TOTAL_SALE_DISCOUNTED_PRICE"))

    top5SubCategoryTotalRevContri
  }

  //Query 3 : Find best selling items (as per customer count) by state
  //In case if there are multiple best selling items, choose any one
  def countOfUsersPerProduct(nMonthsDataDf: DataFrame): DataFrame ={
    val byStoreNameItemName = Window.partitionBy("STORE_STATE").orderBy('CUSTOMER_COUNT.desc)

    val countOfUsersPerProductDf = nMonthsDataDf.
      groupBy("STORE_STATE", "RECEIPT_DESCRIPTION").
      agg(countDistinct("USER_ID").as("CUSTOMER_COUNT")).
      sort(desc("CUSTOMER_COUNT"))

    //Get best selling product per state
    val topSellingProductByState = countOfUsersPerProductDf.
      withColumn("rowNum", row_number().over(byStoreNameItemName)).
      where("rowNum == 1").
      drop("rowNum")

    topSellingProductByState
  }
}