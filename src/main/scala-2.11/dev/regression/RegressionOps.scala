package dev.regression

import dev.data_load.{ComputeStatistics, Csvload, DataWrangle, RemoveNulls}
import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by lucieburgess on 05/12/2017.
  */
object RegressionOps {

  val defaultParams = RegressionParams()
  val rawInput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/db_1489678713_raw.csv"
  val cleanInput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/joinedDFnumeric50.csv"
  val removeNullsInput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/outputfullfiltered50.csv"
  val predictionTest: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/predictionTest.csv"
  val predictionTrain: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/predictionTrain.csv"


  def run(params: RegressionParams): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Basic_Regression_UrbanMindPilot with $params").getOrCreate()

    import spark.implicits._

    println(s"Basic regression Urban Mind pilot data with parameters: \n$params")

    /**
      * Load cleaned data file - avoids having to run the cleaning operation every time
      * Alternatively call: val df = RemoveNulls.runRemoveNulls(DataWrangle.runDataWrangle(file))
      */

    val df = Csvload.createDataFrame(cleanInput) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    //val df = RemoveNulls.runRemoveNulls(df0)

    ComputeStatistics.runComputeStatistics(df)

    df.printSchema()

    val nSamples: Int = df.count().toInt
    println(s"The number of training samples is ${df.count.toInt} ")
    println(s"The number of columns in the dataframe is ${df.columns.length} ")

    val df2 = df.withColumn("impulseInt1", $"baseImpulseScore"*$"201_Can you see trees_numeric")
      .withColumn("impulseInt2", $"baseImpulseScore"*$"202_Can you see the sky_numeric")
      .withColumn("impulseInt3", $"baseImpulseScore"*$"203_Can you hear birds singing_numeric")
      .withColumn("impulseInt4", $"baseImpulseScore"*$"204_Can you see or hear water_numeric")
      .withColumn("impulseInt5", $"baseImpulseScore"*$"205_Do you feel in contact with nature_numeric")

    /** Set up the logistic regression pipeline */
    val pipelineStages = new mutable.ArrayBuffer[PipelineStage]()

    /** Choose the features to be passed into the model */
    val featureCols = Array(
      "baseWellBeingScore",
      //"baseImpulseScore",
      //"001_Age",
      //"002_Gender_numeric",
      //"003_Where did you grow up_numeric",
      //"005_What is your level of education_numeric",
      //"006_Occupation_numeric",
      //"007_How would you rate your physical health overall_numeric",
      "008_How would you rate your mental health overall_numeric",
      "104_Are you indoors or outdoors_numeric",
      "201_Can you see trees_numeric",
      "202_Can you see the sky_numeric",
      "203_Can you hear birds singing_numeric",
      "204_Can you see or hear water_numeric",
      "205_Do you feel in contact with nature_numeric",
      "impulseInt1",
      "impulseInt2",
      "impulseInt3",
      "impulseInt4",
      "impulseInt5")


    val featureAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    pipelineStages += featureAssembler

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("momWellBeingScore")
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)
      .setMaxIter(params.maxIter)
      .setTol(params.tol)
      .setStandardization(params.standardParam)

    pipelineStages += lr
    println("LinearRegression parameters:\n" + lr.explainParams() + "\n")

    /** Randomly split data into test, train with 50% split */
    val train: Double = 1 - params.fracTest
    val test: Double = params.fracTest
    val Array(trainData, testData) = df2.randomSplit(Array(train, test), seed = 123)

    /** Set the pipeline from the pipeline stages */
    val pipeline: Pipeline = new Pipeline().setStages(pipelineStages.toArray)

    /** Fit the pipeline, which includes training the model */
    val startTime = System.nanoTime()
    val pipelineModel: PipelineModel = pipeline.fit(trainData)
    //val lrModel = lr.fit(df8)
    val trainingTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $trainingTime seconds")

    /** Print the weights and intercept for linear regression, from the trained model */
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
    println(s"****************Training features are as follows: ******************")
    println(s"Weights: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    /** Now use the training data to estimate the test data and calculate the test MSE */
    /** Make predictions and evaluate the model using BinaryClassificationEvaluator */
    println("Evaluating model and calculating train and test AUROC - larger is better")
    evaluateRegressionModel("Train", pipelineModel, trainData, predictionTrain)
    evaluateRegressionModel("Test", pipelineModel, testData, predictionTest)

    /** Perform cross-validation on the regression */
    //println("Performing cross validation and computing best parameters")
    //performCrossValidation(df, params, pipeline, lr)

    spark.stop()
  }

  /**
    * Singleton version of BinaryClassificationEvaluator so we can use the same instance across the whole model
    * metric can be rmse, mse, Rsquared or mae (mean absolute error) according to the API
    */
  private object SingletonEvaluator {
    val evaluator: Evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("momWellBeingScore")
      .setPredictionCol("prediction")

    def getEvaluator: Evaluator = evaluator

    //def getMetricName: Evaluator = evaluator.getMetricName
  }

  /**
    * Evaluate the given ClassificationModel on data. Print the results
    *
    * @param model Must fit ClassificationModel abstraction, with Transformers and Estimators
    * @param df    DataFrame with "prediction" and labelColName columns
    */
  private def evaluateRegressionModel(modelName: String, model: Transformer, df: DataFrame, output: String): Unit = {

    val startTime = System.nanoTime()
    val predictions = model.transform(df).cache()
    val predictionTime = (System.nanoTime() - startTime) / 1e9
    println(s"Running time: $predictionTime seconds")
    predictions.printSchema()

    val selected: DataFrame = predictions.select("momWellBeingScore", "features", "prediction")
    selected.show()

    val evaluator = SingletonEvaluator.getEvaluator

    val error = evaluator.evaluate(predictions)

    println(s"Regression results for $modelName: ")
    println(s"The accuracy of the model $modelName is $error") // FIXME get metric name
  }

  /**
    * Perform cross validation on the data and select the best pipeline model given the data and parameters
    *
    * @param data     the dataset for which CV is being performed
    * @param params   the parameters that can be set in the Pipeline - currently LogisticRegression only, may need to amend
    * @param pipeline the pipeline to which cross validation is being applied
    * @param lr       the LogisticRegression model being cross-validated
    */
  private def performCrossValidation(data: DataFrame, params: RegressionParams, pipeline: Pipeline, lr: LinearRegression): Unit = {

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(params.regParam, 0.01, 0.1))
      .addGrid(lr.maxIter, Array(10, 50, params.maxIter))
      .addGrid(lr.elasticNetParam, Array(params.elasticNetParam, 0.5, 1.0))
      .addGrid(lr.standardization, Array(params.standardParam, true))
      .addGrid(lr.tol, Array(params.tol, 0))
      .build()

    println(s"ParamGrid size is ${paramGrid.size}")

    val evaluator = SingletonEvaluator.getEvaluator

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    println("Performing cross-validation on the dataset")
    val cvStartTime = System.nanoTime()
    val cvModel = cv.fit(data)
    val cvPredictions = cvModel.transform(data)
    println(s"Parameters of this CV instance are ${cv.explainParams()}")
    cvPredictions.select("momWellBeingScore", "features", "prediction")
      .show(10)

    evaluator.evaluate(cvPredictions)
    val crossValidationTime = (System.nanoTime() - cvStartTime) / 1e9
    println(s"Cross validation time: $crossValidationTime seconds")

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val avgParams = cvModel.avgMetrics

    val bestLRParams = bestModel.stages.last.asInstanceOf[LinearRegressionModel].explainParams

    println(s"The best model is ${bestModel.toString()} and the params are $bestLRParams")

  }
}




