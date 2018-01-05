package dev.regression

import dev.data_load.DataWrangle
import org.apache.spark.sql.SparkSession

/**
  * Created by lucieburgess on 05/12/2017.
  */
object RegressionOps {

  val defaultParams = RegressionParams()
  val input: String = defaultParams.input
  val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/outputfull.csv"

  def run(params: RegressionParams): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Basic_Regression_UrbanMindPilot with $params").getOrCreate()

    import spark.implicits._

    println(s"Basic regression Urban Mind pilot data with parameters: \n$params")

    val df7 = DataWrangle.runDataWrangle()

  }
}


