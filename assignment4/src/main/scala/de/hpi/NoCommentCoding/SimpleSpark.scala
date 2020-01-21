package de.hpi.NoCommentCoding

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_add, lit}

object SimpleSpark extends App {


  // Turn off logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create a SparkSession to work with Spark
  val sparkBuilder = SparkSession
    .builder()
    .appName("SparkTutorial")
    .master("local[4]") // local, with 4 worker cores
  val spark = sparkBuilder.getOrCreate()

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println(s"Execution: ${t1 - t0} ms")
    result
  }

  // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
  spark.conf.set("spark.sql.shuffle.partitions", "8") //

  // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
  import spark.implicits._
  import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, col, date_add}
  import org.apache.spark.sql.functions._


  //val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
  val inputs = List("region", "nation")
    .map(name => s"src/data/TPCH/tpch_$name.csv")

  time { Sindy.discoverINDs(inputs, spark)}
}
