package dev.data_load

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

  def createDataFrame(inputpath: String): Option[DataFrame] = {

    import spark.implicits._

    val df: Option[DataFrame] = Try(spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputpath)
      .toDF()
      .cache())
      .toOption
    df
  }
}
