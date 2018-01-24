package dev.data_load

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lucieburgess on 24/01/2018.
  * Computes basic statistics for the baseline and momentary data, such as number of participants answering each question
  * and the correlation coefficients between the variables
  */
object ComputeStatistics {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(s"Data cleaning object").getOrCreate()

  import spark.implicits._

  def runComputeStatistics(df: DataFrame) :Unit = {

    /** counts results according to different answers for momentary assessments */
    def countResults (column: String) :Unit = {
      println(s"Displaying momentary assessment results for $column")
      df.select(count(when(lit(column) === "yes", true))).show()
      df.select(count(when(lit(column) === "no", true))).show()
      df.select(count(when(lit(column) === "not sure", true))).show()
      df.select(count(when(lit(column) === "no_data", true))).show()
      df.select(count(lit(column))).show()
    }

    /** Reproduce sample sizes for participants who completed > 50% assessments - move this to a 'ComputeResults' class */
    println("Number of momentary assessments indoors, outdoors or no data:")
    df.select(count(when($"104_Are you indoors or outdoors" === "Indoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "Outdoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "no_data", true))).show


    countResults("201_Can you see trees") // 1213, 800, 29, 0
    countResults("202_Can you see the sky")
    countResults("203_Can you hear birds singing")
    countResults("204_Can you see or hear water")
    countResults("205_Do you feel in contact with nature")


    //FIXME need to continue this for the other measurements
  }

}
