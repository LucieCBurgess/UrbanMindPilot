package dev.regression

import dev.data_load.{Csvload, ScoreMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Created by lucieburgess on 05/12/2017.
  */
object DataWrangle {

  val defaultParams = RegressionParams()
  val input: String = defaultParams.input
  val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/output_test.csv"

  def run(params: RegressionParams): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Basic_Regression_UrbanMindPilot with $params").getOrCreate()

    import spark.implicits._

    println(s"Basic regression Urban Mind pilot data with parameters: \n$params")

    /** Load training and test data and cache it */
    val df = Csvload.createDataFrame(input) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    df.printSchema()
    println(s"Raw dataset contains ${df.count()} rows")
    println(s"Raw dataset contains ${df.columns.length} columns")

    val addLeadingZeros: (Int => String) = s => "%03d".format(s)
    val newCol = udf(addLeadingZeros).apply(col("QuestionId"))

    val df2 = df.filter($"QuestionId" <2000)
      .withColumn("Q_id_new", newCol)
      .withColumn("Q_id_string", concat($"Q_id_new", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("Q_id_string"))
      .withColumn("ValidatedResponse", when($"AnswerValue".startsWith("None"), $"AnswerText").otherwise($"AnswerValue"))

    // we can't use ValidatedResponse as AnswerValue calculates the trait impulsivity score incorrectly
    // Will have to go back to correctly calculating AnswerValue from the AnswerText column

    val df3 = df2.withColumn("Q_id_string_cleaned", regexp_replace(df2.col("Q_id_string"),"[\\',\\?,\\:,\\.]","")) // comma too?

    val columnNames: Array[String] = df3.select($"Q_id_string_cleaned").distinct.as[String].collect.sortWith(_<_)

    val df4 = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd")
      .pivot("Q_id_string_cleaned")
      .agg(first("ValidatedResponse"))
      .orderBy("participantUUID", "assessmentNumber")

    val reorderedColumnNames: Array[String] = Array("participantUUID","assessmentNumber","geotagStart","geoTagEnd") ++ columnNames

    val df5: DataFrame = df4.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    println(s"Cleaned pivoted dataset contains ${df5.count()} rows")
    println(s"Cleaned pivoted dataset contains ${df5.columns.length} columns")

    /** Now calculate the Edinburgh-Warwick wellbeing score at baseline */

    val baseWellBeingCols = List(col("041_Ive been feeling optimistic about the future"),col("042_Ive been feeling useful"),
        col("043_Ive been feeling relaxed"), col("044_Ive been feeling interested in other people"),col("045_Ive had energy to spare"),
        col("046_Ive been dealing with problems well"),col("047_Ive been thinking clearly"),col("048_Ive been feeling good about myself"),
        col("049_Ive been feeling close to other people"), col("050_Ive been feeling confident"), col("051_Ive been able to make up my own mind about things"),
        col("052_Ive been feeling loved"),col("053_Ive been interested in new things"),col("054_Ive been feeling cheerful"))

    val df6 = df5.withColumn("baseWellbeingScore", baseWellBeingCols.reduce(_+_))

    /** Now calculate the Edinburgh-Warwick wellbeing score during momentary assessments */

    val momWellBeingCols = List(col("131_Right now I feel optimistic about the future"), col("132_Right now I feel useful"),
      col("133_Right now I feel relaxed"), col("134_Right now I feel interested in other people"),col("135_Right now I have energy to spare"),
      col("136_Right now I deal with problems well"), col("137_Right now I think clearly"), col("138_Right now I feel good about myself"),
      col("139_Right now I feel close to other people"), col("140_Right now I feel confident"), col("141_Right now I am able to make up my own mind about things"),
      col("142_Right now I feel loved"), col("143_Right now I am interested in new things"),col("144_Right now I feel cheerful"))

    val df7 = df6.withColumn("momWellbeingScore", momWellBeingCols.reduce(_+_))

    df7.select($"participantUUID", $"assessmentNumber", $"baseWellBeingScore", $"momWellBeingScore").show()

    writeDFtoCsv(df7,output)
    df7.printSchema()

    /** Now calculate trait impulsivity */


    //val df8 = df7.withColumn("impulsivity", )

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



