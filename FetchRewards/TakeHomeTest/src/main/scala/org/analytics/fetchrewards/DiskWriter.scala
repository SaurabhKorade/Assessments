package org.analytics.fetchrewards

import org.apache.spark.sql.DataFrame

class DiskWriter {

  //Accept output path from user or write to project resources directory
  def writeToDisk(resultDf: DataFrame, format: String): Unit = {
    if (format.equalsIgnoreCase("csv")) {
      resultDf.write.csv("")
    }
    else if (format.equalsIgnoreCase("json")) {
      resultDf.write.parquet("")
    }
    else if (format.equalsIgnoreCase("parquet")) {
      resultDf.write.json("")
    }
  }

}
