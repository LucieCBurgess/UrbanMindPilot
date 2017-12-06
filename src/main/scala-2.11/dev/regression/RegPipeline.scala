package dev.regression

import dev.data_load.Csvload
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Created by lucieburgess on 05/12/2017.
  */
object RegPipeline {

  val defaultParams = RegressionParams()
  val inputpath: String = defaultParams.input
  val outputpath: String = defaultParams.output

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
    println(s"Raw dataset contains ${df.count()} lines")

    /**
      * AnswerValue should be numeric but contains strings "None"
      * This maps the DF to a column "numeric_answer" which is of IntegerType
      * Allows the DF to be grouped (by assessmentNumber, participant UUID) and pivoted (by Question) so that we can work with the data
      * However still need to add back the remaining columns: Geotag start, Geotag end, etc
      */
    val df2 = df.withColumn("numeric_answer", when($"AnswerValue".startsWith("None"), 0).otherwise($"AnswerValue").cast(IntegerType))

    val df3 = df2.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"),asc("assessmentNumber"),asc("QuestionId"))

    val rawOutput: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/raw_ordered.csv"

    writeDFtoCsv(df3,rawOutput)

    //df3.select("QuestionID","Question","ParticipantUUID","numeric_answer").show(34) //testing purposes
    df3.printSchema()

    val df4 = df3.groupBy("participantUUID","assessmentNumber","geotagStart","geotagEnd")
      .pivot("Q_id_string")
      .sum("numeric_answer")
      .orderBy("participantUUID","assessmentNumber")

    df4.show()

    writeDFtoCsv(df4,outputpath)

    //val df4 = df3.join(df2,Seq("participantUUID","assessmentNumber","Question")).show(10) // no ref on LHS of join


  } //run

  /** Helper functions */

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }

}



