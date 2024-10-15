package com.tarique.project1
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql._

//Iris Pipeline
object Iris extends IrisWrapper {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.StringIndexer

    val indexer = new StringIndexer()
      .setInputCol(irisFeatures_CategoryOrSpecies_IndexedLabel._2)
      .setOutputCol(irisFeatures_CategoryOrSpecies_IndexedLabel._3)

    val dataSet = buildDataFrame(dataSetPath)

    //Split the dataset in two. 85% of the dataset becomes the Training (data) set and 15% becomes the testing (data) set
    val splitDataSet: Array[Dataset[Row]] = dataSet.randomSplit(Array(0.85, 0.15), 98765L)
    println("Size of the new split dataset " + splitDataSet.size)

    val trainDataSet = splitDataSet(0)
    println("TRAIN DATASET set count is: " + trainDataSet.count())
    val testDataSet = splitDataSet(1)
    println("TEST DATASET set count is: " + testDataSet.count())

    //session.stop()
    //Create a random forest classifier
    val randomForestClassifier = new RandomForestClassifier()
      .setFeaturesCol(irisFeatures_CategoryOrSpecies_IndexedLabel._2)
      .setFeatureSubsetStrategy("sqrt")

    //Start building the pipeline, stages are "Indexer" and "Classifier"
    val irisPipeline = new Pipeline().setStages(Array[PipelineStage](indexer) ++ Array[PipelineStage](randomForestClassifier))



  }
}
