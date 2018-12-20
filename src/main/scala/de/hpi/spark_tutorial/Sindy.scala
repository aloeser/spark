package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession


object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession, cores: Int): Unit = {
    import spark.implicits._

    val dataframes = inputs.map(file => spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("quote", "\"")
      .csv(file).toDF)

    val zipped = dataframes.map(df => {
      val headers = df.columns//.map(s => List(s));
      df.flatMap(row => {
        row.toSeq.asInstanceOf[Seq[String]].zip(headers)
      })
    })

    val cells = zipped.reduce(_ union _)

    val cache_preaggregated = cells.rdd.groupByKey().mapValues(_.toSet)

    val global_partitioning = cache_preaggregated.repartition(2*cores)

    val attribute_sets = global_partitioning.reduceByKey(_ union _).map(_._2).distinct


    val inclusion_lists = attribute_sets.flatMap(set => {set.map(word => Tuple2(word, set - word))})

    val aggregates = inclusion_lists.reduceByKey(_ intersect _).filter(_._2.size > 0)
    val output_lines = aggregates.map(aggregate => {aggregate._1 + " < " + aggregate._2.mkString(", ")})
    output_lines.collect().sorted.foreach(println(_))

    //val ind = aggregates.flatMap(aggregate => { val subset = aggregate._1; aggregate._2.map(Tuple2(subset, _))}).distinct
  }
}

