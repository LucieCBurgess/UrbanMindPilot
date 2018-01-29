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

  val res1 = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/result1.txt"

  def runComputeStatistics(df: DataFrame) :Unit = {

    /** counts results according to different answers for momentary assessments */
    def countResults (colname: String) :Unit = {
      println(s"Displaying momentary assessment results for $colname")
      df.select(count(when(col(colname) === "yes", true))).show()
      df.select(count(when(col(colname) === "no", true))).show()
      df.select(count(when(col(colname) === "not sure", true))).show()
      df.select(count(when(col(colname) === "no_data", true))).show()
      df.select(count(lit(colname))).show()
    }

    /** Reproduce sample sizes for participants who completed > 50% assessments - move this to a 'ComputeResults' class */
    println("Number of momentary assessments indoors, outdoors or no data:")
    df.select(count(when($"104_Are you indoors or outdoors" === "Indoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "Outdoors", true))).show
    df.select(count(when($"104_Are you indoors or outdoors" === "no_data", true))).show


    countResults("201_Can you see trees") // 1211, 800, 29, 0 = 2040
    countResults("202_Can you see the sky") // 1219, 543, 13, 265 = 2040
    countResults("203_Can you hear birds singing") // 115, 133, 19, 1773 = 2040
    countResults("204_Can you see or hear water") // 56, 204, 7, 1773
    countResults("205_Do you feel in contact with nature") // 152, 89, 25, 1774


    //FIXME need to continue this for the other measurements
  }

}
