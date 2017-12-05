package dev.data_load

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Try

/**
  * Created by lucieburgess on 20/10/2017.
  */
object Csvload {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Data_load_pilot_csv")
    .getOrCreate()

  def createDataFrame(path: String): DataFrame = {

    import spark.implicits._

    //val path: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_export_Sept_2017_nocalcs.csv"

    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    df
  }
}
