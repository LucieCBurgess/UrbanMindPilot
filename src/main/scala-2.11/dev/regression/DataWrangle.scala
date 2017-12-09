package dev.regression

import dev.data_load.Csvload
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Created by lucieburgess on 05/12/2017.
  */
object DataWrangle {

  val defaultParams = RegressionParams()
  val inputpath: String = defaultParams.input

  def run(params: RegressionParams): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Basic_Regression_UrbanMindPilot with $params").getOrCreate()

    import spark.implicits._

    println(s"Basic regression Urban Mind pilot data with parameters: \n$params")

    /** Load training and test data and cache it */
    val df = Csvload.createDataFrame(inputpath) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    df.printSchema()
    println(s"Raw dataset contains ${df.count()} rows")
    println(s"Raw dataset contains ${df.columns.length} columns")

    /**
      * Pivots the dataframe so that each question is a column in the DF
      * Still need to combine 'AnswerText' and 'AnswerValue' columns
      */
    val df2 = df.withColumn("numeric_answer", when($"AnswerValue".startsWith("None"), 0).otherwise($"AnswerValue").cast(IntegerType))

    val df3 = df2.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"),asc("assessmentNumber"),asc("QuestionId"))

    val rawOutput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/raw_ordered_test.csv"

    writeDFtoCsv(df3,rawOutput)
    df3.printSchema()


    val df4 = df3.groupBy("participantUUID","assessmentNumber","geotagStart","geotagEnd")
      .pivot("Q_id_string") // and pivot by question?
      .agg(first("AnswerText")) //solves the aggregate function must be numeric problem
      .orderBy("participantUUID","assessmentNumber")

    df4.show()

    val pivotedOutput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/output_test.csv"

    writeDFtoCsv(df4,pivotedOutput)
  }

  /** Helper functions */

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }

}



