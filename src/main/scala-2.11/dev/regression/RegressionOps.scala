package dev.regression

import dev.data_load.{Csvload, DataWrangle}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by lucieburgess on 05/12/2017.
  */
object RegressionOps {

  val defaultParams = RegressionParams()
  //val input: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/outputfull.csv"
  val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/regression.csv"

  def run(params: RegressionParams): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Basic_Regression_UrbanMindPilot with $params").getOrCreate()

    import spark.implicits._

    println(s"Basic regression Urban Mind pilot data with parameters: \n$params")

    /**
      * Load cleaned data file - avoids having to run the cleaning operation every time
      * Alternatively call: val df: DataFrame = DataWrangle.runDataWrangle()
      */
//    val df = Csvload.createDataFrame(input) match {
//      case Some(dfload) => dfload
//      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
//    }

    val df = DataWrangle.runDataWrangle()

    sys.exit() // Included for testing. Remove this when we want to get on to the regression

    /** Filter out all assessments not at baseline */
    val df2: DataFrame = df.filter($"baseWellBeingScore" > 0)

    df2.select($"participantUUID",$"assessmentNumber",$"geotagStart",$"geoTagEnd",$"baseWellBeingScore").show(100)

    df2.printSchema()

    val nSamples: Int = df2.count().toInt
    println(s"The number of training samples is $nSamples")

    /** Set up the logistic regression pipeline */
    val pipelineStages = new mutable.ArrayBuffer[PipelineStage]()

    //FIXME
    // Two problems: Strings must be converted to numeric types - Vector Assembler cannot deal with strings (obviously)
    // Vector Assembler can't deal with nulls -
    // see https://stackoverflow.com/questions/41362295/sparkexception-values-to-assemble-cannot-be-null
    // I've commented out the String columns, let's see if we can get a regression model working on the numeric questions
    // to start with
    val featureCols = Array(
//      "001_Age",
//      "002_Gender",
//      "003_Where did you grow up",
//      "004_How many years have you lived in the city",
//      "005_What is your level of education",
//      "006_Occupation",
//      "007_How would you rate your physical health overall",
//      "008_How would you rate your mental health overall",
      "013_Are you normally aware of the urban environment around you",
      "014_Do you walk from A to B as quickly as possible",
      "015_Do you meander and daydream as you walk",
      "016_Do you take detours through parks and green spaces",
      "017_Do you eat on the move",
      "018_Are you bothered when people litter",
      "019_Do you follow rules ie keep off the grass",
      "020_Do you stick to travel routes that you know",
      "021_Do you give way to others in the street",
      "022_Do you change your route after dark",
      "024_How many hours do you spend online for work / study each day",
      "025_How many hours do you spend online for recreation each day",
      "027_How often do you have a drink containing alcohol",
      //   "028_How many units of alcohol do you drink on a typical day when you are drinking", parses as string, why?
      "029_How often have you had 6 or more units if female or 8 or more if male on a single occasion in the last year")

    val featureAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    pipelineStages += featureAssembler

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("baseWellbeingScore")
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)
      .setMaxIter(params.maxIter)
      .setTol(params.tol)

    pipelineStages += lr
    println("LinearRegression parameters:\n" + lr.explainParams() + "\n")

    /** Set the pipeline from the pipeline stages */
    val pipeline: Pipeline = new Pipeline().setStages(pipelineStages.toArray)

    /** Fit the pipeline, which includes training the model */
    val startTime = System.nanoTime()
    val pipelineModel: PipelineModel = pipeline.fit(df2) //val lrModel = lr.fit(df8)
    val trainingTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $trainingTime seconds")

    /** Print the weights and intercept for linear regression, from the trained model */
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
    println(s"Training features are as follows: ")
    println(s"Weights: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  }
}


