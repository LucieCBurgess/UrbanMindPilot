package dev.regression

import dev.data_load.Csvload
import org.apache.spark.sql.functions.when
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
      * However still need to add back:
      */
    val df2 = df.withColumn("numeric_answer", when($"AnswerValue".startsWith("None"), 0).otherwise($"AnswerValue").cast(IntegerType))

    df2.select("QuestionID","Question","ParticipantUUID","numeric_answer").show(34) //testing purposes
    df2.printSchema()

    val df3 = df2.groupBy("participantUUID","assessmentNumber").pivot("Question").sum("numeric_answer").orderBy("participantUUID")
    df3.show()

    val df4 = df3.join(df2,Seq("participantUUID","assessmentNumber"))



  } //run

  //def convertTextToLikertVals(column: Column): Unit



}



