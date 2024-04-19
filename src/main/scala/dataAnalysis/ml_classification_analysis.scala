package dataAnalysis

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object ml_classification_analysis {
  def mlMethod(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Data Analysis")
      .master("local[*]")
      .getOrCreate()

    // need to change the path if generated new csv
    val df = spark.read
      .option("header", "true")
      .csv("src/main/resources/output/csvOutput/part-00000-977de863-f78c-40b8-9944-6dcc2b9993f0-c000.csv")

    val indexer = new StringIndexer()
      .setInputCol("consumer_disputed")
      .setOutputCol("label")

    // 选择几个可能有用的特征列，并对分类变量进行编码
    val featureCols = Array("product", "issue", "state", "zip_code", "date_received", "date_sent_to_company")
    val featureIndexers = featureCols.map(
      columnName => new StringIndexer().setInputCol(columnName).setOutputCol(columnName + "_indexed")
    )

    // 创建特征向量
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.map(_ + "_indexed"))
      .setOutputCol("features")

    // 创建模型
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    // 创建和运行Pipeline
    val pipeline = new Pipeline()
      .setStages(featureIndexers :+ assembler :+ indexer :+ lr)

    // 划分数据集
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3), seed = 1234)

    // 训练模型
    val model = pipeline.fit(trainingData)

    // 测试模型
    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(5)
  }
}
