package dataAnalysis

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, StopWordsRemover}

object TextAnalysis extends App {
  val spark = SparkSession.builder
    .appName("Text Classification")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Read the parquet file into a DataFrame
  val parquetFile = "src/main/resources/data_19601.parquet"
  val dataFrame = spark.read.parquet(parquetFile).cache()

  // Define a UDF to create a label based on whether the complaint text contains certain keywords
  val labelFunction: String => Int = text =>
    if (text.toLowerCase.contains("fraud") || text.toLowerCase.contains("unauthorized")) 1 else 0
  val labelUDF = udf(labelFunction)

  // Add the label column to the DataFrame
  val labeledDataFrame = dataFrame.withColumn("label", labelUDF(col("complaint_what_happened")))

  // Define the ML pipeline
  val tokenizer = new Tokenizer().setInputCol("complaint_what_happened").setOutputCol("words")
  val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
  val hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures")
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

  val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf, lr))

  // Split the data into training and test sets
  val Array(trainingData, testData) = labeledDataFrame.randomSplit(Array(0.7, 0.3))

  // Train the model
  val model = pipeline.fit(trainingData)

  // Make predictions on the test data
  val predictions = model.transform(testData)

  // Select example rows to display
  predictions.select("complaint_what_happened", "label", "features", "prediction").show(10)

  // Evaluate the model by computing the accuracy
  val correctPredictions = predictions.where($"label" === $"prediction").count()
  val totalData = testData.count()
  println(s"Accuracy: ${correctPredictions.toDouble / totalData}")

  spark.stop()
}