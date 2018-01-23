package dev.regression

import dev.data_load.{Csvload, DataWrangle, RemoveNulls}
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

    //val df = DataWrangle.runDataWrangle()

    val df = RemoveNulls.runRemoveNulls()

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
    val featureCols = Array(
      "001_Age_numeric",
      "002_Gender_numeric",
      "003_Where did you grow up_numeric",
      "004_How many years have you lived in the city_numeric",
      "005_What is your level of education_numeric",
      "006_Occupation_numeric",
      "007_How would you rate your physical health overall_numeric",
      "008_How would you rate your mental health overall_numeric")


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


