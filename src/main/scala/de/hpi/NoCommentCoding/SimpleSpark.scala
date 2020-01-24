package de.hpi.NoCommentCoding

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleSpark extends App {

  // Turn off logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var dataPath = "./TPCH"
  var numCores  = 4

  try {
    for (i <- args.indices) {
      if (args(i) == "--path") {
        dataPath = args(i + 1)
      } else if (args(i) == "--cores") {
        numCores = args(i + 1).toInt
      }
    }
  } catch {
    case _: Throwable => {
      println(" Usage: java -jar NoCommentCoding.jar [--path ./TPCH] [--cores 4]")
      println(" Hint: Scala requires Java 8")
      System.exit(1)
    }
  }

  // Create a SparkSession to work with Spark
  val sparkBuilder = SparkSession
    .builder()
    .appName("sINDy")
    .master(s"local[$numCores]") // local, with 4 worker cores
  val spark = sparkBuilder.getOrCreate()

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println(s"Execution: ${t1 - t0} ms")
    result
  }

  val numPartitions = Math.min(numCores * 4, 200)

  // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
  spark.conf.set("spark.sql.shuffle.partitions", s"$numPartitions") //

  val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
    .map(name => s"$dataPath/tpch_$name.csv")

  time { Sindy.discoverINDs(inputs, spark, numPartitions) }
}
