package de.hpi.NoCommentCoding

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    println(inputs)


    def read(name: String) = {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .format("com.databricks.spark.csv")
        .load(name)
    }

    val files = inputs.map(read)

    files.foreach(f => f.show(5, truncate = true))

  }
}
