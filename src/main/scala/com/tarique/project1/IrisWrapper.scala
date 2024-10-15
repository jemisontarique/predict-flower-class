package com.tarique.project1

import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait IrisWrapper {
  //Preprocessing, Data Transformation, and DataFrame Creation

  //The entry point to programming Spark with the Dataset and DataFrame API.
  //This is the SparkSession
  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Iris-Pipeline")
      .getOrCreate()
  }
  val dataSetPath = "/Users/tariquejemison/Documents/ModernScalaProjects/IrisDataset/iris.csv"
  protected val irisFeatures_CategoryOrSpecies_IndexedLabel = ("iris-features-column","iris-species-column", "label")

  def buildDataFrame(dataSet: String): DataFrame = {
    def getRows: Array[(Vector, String)] = {
      session.sparkContext.textFile(dataSet).flatMap{
        partition => partition.split("\n").toList
      }.map(_.split(","))
        .collect.drop(1)
        .map(row => (Vectors.dense(row(1).toDouble,row(2).toDouble,row(3).toDouble,row(4).toDouble),row(5)))
    }
    val dataFrame = session.createDataFrame(getRows).toDF(irisFeatures_CategoryOrSpecies_IndexedLabel._1, irisFeatures_CategoryOrSpecies_IndexedLabel._2)
    dataFrame
  }
}
