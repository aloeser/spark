package de.hpi.spark_tutorial

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sindy {



  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    var dataframes : List[DataFrame] = List()

    for (input <- inputs) {
      dataframes = dataframes :+ (spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("quote", "\"")
        .csv(input).toDF)
    }
    import spark.implicits._
    var cells: DataFrame = spark.createDataFrame(,List(("value", StringType, true), ("columnname", StringType, true)))
    for (dataframe <- dataframes) {
      val columns = dataframe.columns
      dataframe.foreach(row => {

        for (index <- row.toSeq.indices) {
          spark.createDataset(List()).
        }
      })
    }
  }
}
