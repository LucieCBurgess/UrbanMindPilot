package dev.data_load

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

/**
  * Created by lucieburgess on 24/01/2018.
  * Computes basic descriptive statistics for the baseline and momentary data, such as number of participants answering each question
  * on input files of format joinedDFnumeric50. For correlation coefficients see the R script.
  * i.e. filtered, pivoted, ordered, nulls removed, baseline assessments appearing as fixed columns instead of rows.
  */
object ComputeStatistics {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(s"Data cleaning object").getOrCreate()

  import spark.implicits._

  val res1 = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/result1.txt"

  def runComputeStatistics(df: DataFrame) :Unit = {

    df.select(countDistinct($"momParticipantUUID")).show() // number of participants

    calculateTotals(df, "002_Gender") // male vs. female

    calculateMeanBaseColumn(df, "001_Age") // Mean age

    calculateTotals(df, "003_Where did you grow up")
    calculateTotals(df, "006_Occupation")
    calculateTotals(df, "005_What is your level of education")
    calculateTotals(df, "007_How would you rate your physical health overall")
    calculateTotals(df, "008_How would you rate your mental health overall")

    calculateMeanBaseColumn(df, "baseWellBeingScore") // mean base wellbeing score

    df.select(mean($"momWellBeingScore")).show() // mean momentary wellbeing score

    calculateMeanBaseColumn(df, "baseImpulseScore") // mean trait impulsivity score at baseline

    calculateMeanAssessments(df) // mean number of assessments

    /** Calculate number of momentary assessments, indoors or outdoors */
    df.select(count(when($"104_Are you indoors or outdoors" === "Indoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "Outdoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "NA", true))).show


    countResults(df, "201_Can you see trees") // 1211, 800, 29, 0 = 2040
    countResults(df, "202_Can you see the sky") // 1219, 543, 13, 265 = 2040
    countResults(df, "203_Can you hear birds singing") // 115, 133, 19, 1773 = 2040
    countResults(df, "204_Can you see or hear water") // 56, 204, 7, 1773
    countResults(df, "205_Do you feel in contact with nature") // 152, 89, 25, 1774

    }

  // ********************** Helper functions *****************************************

  /** counts results according to different answers for momentary assessments */
  def countResults (df: DataFrame, colname: String) :Unit = {
    println(s"Displaying momentary assessment results for $colname")
    df.select(count(when(col(colname) === "yes", true))).show()
    df.select(count(when(col(colname) === "no", true))).show()
    df.select(count(when(col(colname) === "not sure", true))).show()
    df.select(count(when(col(colname) === "NA", true))).show()
    df.select(count(lit(colname))).show()
  }

  def calculateMeanBaseColumn(df: DataFrame, colName: String) : Unit = {
    val df2 = df.select($"momParticipantUUID", col(colName))
      .groupBy($"momParticipantUUID", col(colName))
      .agg(count(col(colName)))

    val df3 = df2.select(mean(col(colName))).show()
  }

  def calculateMeanAssessments(df: DataFrame): Unit =  {
    df.select($"momParticipantUUID",$"momAssessmentNumber")
      .groupBy($"momParticipantUUID")
      .agg(countDistinct("momAssessmentNumber") as "totalAssessments")
      .agg(mean($"totalAssessments"))
      .show()
  }

  def calculateTotals(df: DataFrame, colName: String) : Unit = {
    val df2 = df.select($"momParticipantUUID",col(colName))
      .groupBy($"momParticipantUUID", col(colName))
      .agg(countDistinct(col(colName)) as colName.concat("_totals"))

    val df3 = df2.select(col(colName))
       .groupBy(col(colName))
       .agg(count(col(colName)) as colName.concat("_total")).show()
  }

}
