package de.hpi.spark_tutorial

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.collection.mutable.ArrayBuffer

object Sindy {



  def discoverINDs(inputs: List[String], spark: SparkSession, cores: Int): Unit = {
    import spark.implicits._

    val dataframes = inputs.map(file => spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("quote", "\"")
      .csv(file).toDF)


    val zipped = dataframes.map(df => {
      val headers = df.columns.map(s => List(s));
      df.flatMap(row => {
        row.toSeq.asInstanceOf[Seq[String]].zip(headers)
      })
    })

    val cells = zipped.reduce(_ union _)

    val cache_preaggregated = cells.rdd.reduceByKey(_ union _)

    val global_partitioning = cache_preaggregated.repartition(2*cores) // TODO

    val attribute_sets = global_partitioning.reduceByKey(_ union _).map(_._2)

    val inclusion_lists = attribute_sets.flatMap(list =>  {list.map(word => Tuple2(word, list.filter(_ != word)))})

    val aggregates = inclusion_lists.reduceByKey(_ intersect _)

    val output_lines = aggregates.map(aggregate => {aggregate._1 + " < " + aggregate._2.mkString(", ")})
    output_lines.foreach(print(_))

    val ind = aggregates.flatMap(aggregate => { val subset = aggregate._1; aggregate._2.map(Tuple2(subset, _))}).distinct

    ind.toDF.show(50)
  }
}

