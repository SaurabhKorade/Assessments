package org.analytics.fetchrewards

import org.apache.spark.sql.DataFrame

class DiskWriter {

  //Writes result to disk in specified format
  def writeToDisk(resultDf: DataFrame, format: String, path: String): Unit = {
    if (format.equalsIgnoreCase("csv")) {
      resultDf.write.csv(path)
    }
    else if (format.equalsIgnoreCase("json")) {
      resultDf.write.parquet(path)
    }
    else if (format.equalsIgnoreCase("parquet")) {
      resultDf.write.json(path)
    }
  }

}
