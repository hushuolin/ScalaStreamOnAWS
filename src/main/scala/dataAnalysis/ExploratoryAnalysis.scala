package dataAnalysis

import org.apache.spark.sql.{SparkSession, functions => F}

object ExploratoryAnalysis extends App {
  // Initialize Spark session
  val spark = SparkSession.builder
    .appName("Data Analysis")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Set the Spark SQL configuration to LEGACY to allow parsing of the old timestamp format
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  // Read the parquet file into a DataFrame
  val parquetFile = "src/main/resources/data_19601.parquet"
  val dataFrame = spark.read.parquet(parquetFile)

  // View the first few records in the DataFrame
  dataFrame.show()

  // Print schema to understand data structure
  dataFrame.printSchema()

  // Summary statistics for "complaint_id"
  println("Summary statistics for complaint_id:")
  dataFrame.describe("complaint_id").show()

  // Count of distinct companies
  println("Distinct companies count: " + dataFrame.select("company").distinct().count())

  // Complaint counts by company
  println("Complaint counts by company:")
  dataFrame.groupBy("company").count().orderBy(F.desc("count")).show()

  // Complaint trends over time
  println("Complaint trends over time:")
  dataFrame.withColumn("received_date", F.to_date(F.col("date_received"), "yyyy-MM-dd'T'HH:mm:ss"))
    .groupBy(F.window(F.col("received_date"), "1 day"))
    .count()
    .orderBy("window")
    .show()

  // State-wise complaint distribution
  println("State-wise complaint distribution:")
  dataFrame.groupBy("state").count().orderBy(F.desc("count")).show()

  // Issue analysis
  println("Common issues faced by consumers:")
  dataFrame.groupBy("issue").count().orderBy(F.desc("count")).show()

  // Impact of submission method on resolution
  println("Impact of submission method on resolution:")
  dataFrame.groupBy("submitted_via", "company_response").count().orderBy(F.desc("count")).show()

  // Perform simple text analysis on narratives for further machnie learning modeling
  if (dataFrame.columns.contains("complaint_what_happened")) {
    println("Performing text analysis on complaint narratives:")
    dataFrame.filter(F.col("has_narrative") === true)
      .select("complaint_what_happened")
      .show(truncate = false)
  }

  // Stop the Spark session
  spark.stop()
}
