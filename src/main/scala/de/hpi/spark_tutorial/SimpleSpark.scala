package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object SimpleSpark extends App {

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println(s"Execution: ${t1 - t0} ms")
    result
  }

  override def main(args: Array[String]): Unit = {


    if (args.length != 0 && args.length != 4) {
      print("Usage: java -jar <FILE> [--path <PATH> --cores <COUNT>]")
      return
    }

    var path = "./TPCH"
    var cores = 4
    if (args.length == 4) {
      path = args(1)
      cores = args(3).toInt
    }


    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._
    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //


    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time { Sindy.discoverINDs(inputs, spark, cores) }
  }
}
