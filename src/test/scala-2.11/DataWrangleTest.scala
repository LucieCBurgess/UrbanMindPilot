import dev.data_load.Csvload
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSuite

/**
  * @author lucieburgess on 08/12/2017
  *  Test of the data pre-processing function which cleans and pivots the raw data file so it's in a suitable format
  *  to work with for analytics.
  */
class DataWrangleTest extends FunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataWrangleTest")
    .getOrCreate()

  import spark.implicits._

  val inputpath = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/db_1489678713_raw_test.csv"

  /** Load dataframe from csv file */
  val df = Csvload.createDataFrame(inputpath) match {
    case Some(dfload) => dfload
    case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
  }

  /** Add a column Q_id_string which concatenates the Q_id and the Question */
  val df2 = df.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
    .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("QuestionId"))

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }

  val outputpath: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/db_1489678713_raw_ordered_test.csv"

  /** -------------------------------- Tests from here ------------------------------------ */

  test("[01] Calling writeDFtoCSv writes the result to a csv file") {
    writeDFtoCsv(df2, outputpath)
  }

  test("[02] Loading test dataframe creates a DF of the correct number of rows") {
    df.printSchema()
    assertResult(480) {
      df.count()
    }
  }

  test("[03] Loading test dataframe creates a DF of the correct number of columns") {
    assertResult(9) {
      df.columns.length
    }
  }

  test("[04] Adding a Q_id_string column concatenates the QuestionId and the Question columns") {
    df2.printSchema()
    assertResult(10) {
      df2.columns.length
    }
  }

  test("[05] Filter out Q_ids relating to games") {

    val df3 = df2.filter($"QuestionId" <2000)
    assertResult(480-60) {
      df3.count()
    }
  }

  test ("[06] Add a new column of StringType which combines the string and numeric answers, and check with SQL queries") {
    val df3 = df2.withColumn("ValidatedResponse", when($"AnswerValue".startsWith("None"), $"AnswerText").otherwise($"AnswerValue"))

    df3.show()
    df3.printSchema()
    df3.createOrReplaceTempView("datatable")

    assertResult(1) {
      spark
        .sql("""SELECT AnswerText
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=0
            AND Q_id_string = '1_Age'""")
        .where($"AnswerText" === "39")
        .count
    }

    val result = spark
      .sql("""SELECT AnswerText
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=0
            AND Q_id_string = '1_Age'""")

    result.printSchema()
    result.show()
    assertResult("39") {
      result.collect.head.getString(0)
    }

    val result2 = spark
      .sql("""SELECT AnswerText
          FROM datatable
          WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
          AND assessmentNumber=0 AND Q_id_string = '2_Gender'""")

    result2.printSchema()
    result2.show()
    assertResult("Female") {
      result2.collect.head.getString(0)
    }

    val result3 = spark
      .sql("""SELECT ValidatedResponse
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=0
            AND Q_id_string = '1_Age'""")

    assert(result.collect.head.getString(0)===result3.collect.head.getString(0))

    val result4 = spark
      .sql("""SELECT ValidatedResponse
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=0
            AND Q_id_string = '14_Do you walk from A to B as quickly as possible?'""")

    assertResult("4") {
      result4.collect.head.getString(0)
    }
  }

  test("[07] Create an array string from the Q_id_string column and use it to pivot dataframe") {

    val questions: Array[String] = df2.select("Q_id_string")
      .distinct()
      .collect()
      .map(_.getAs[String]("Q_id_string"))
      .sortWith(_ < _)

    assertResult(110) {
      questions.length
    }

    println(questions.mkString(","))

    assert(questions(0).equals("101_Who is with you right now?"))
    assert(questions(1).equals("102_What are you doing right now?"))
    assert(questions(questions.length - 1).equals("8_How would you rate your mental health overall?"))

    val df3: DataFrame = questions.foldLeft(df2) {
      case (data, question) =>
        data.withColumn(question, when($"Q_id_string" === question, $"AnswerText"))
    }

    df3.printSchema()

  }

  test("[08] Pivot dataframe using Q_id_string and re-order it in order of Question") {

    val df3 = df2.withColumn("ValidatedResponse", when($"AnswerValue".startsWith("None"), $"AnswerText").otherwise($"AnswerValue"))

    val df4 = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd")
      .pivot("Q_id_string") // and pivot by question?
      .agg(first("ValidatedResponse")) //solves the aggregate function must be numeric problem
      .orderBy("participantUUID", "assessmentNumber")

    assertResult(114) {
      df4.columns.length
    }

    val numberOfRows: Int = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd").count().distinct().collect().length

    assert(df4.count() === numberOfRows)

  }

  test("[09] Add leading zeros to the QuestionId") {

    val addLeadingZeros: (Int => String) = s => "%03d".format(s)

    val newCol = udf(addLeadingZeros).apply(col("QuestionId")) // creates the new column
    val df3 = df2.withColumn("Q_id_new", newCol) // adds the new column to original

    df3.printSchema()
    df3.show
  }

  test("[10] Create a new DataFrame with the columns we want and cache it") {
    val columns: Array[String] = df2.columns
    val reorderedColumnNames: Array[String] = Array("participantUUID","assessmentNumber","Q_id_string","AnswerText","AnswerValue","geotagStart","geoTagEnd")
    val result: DataFrame = df2.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    result.printSchema()

  }


    //    // wrap it up - intrigued to see if we can make this work ...
    //    val result = withQIDColumns
    //      .drop("Q_id_string")
    //        .drop("QuestionId")
    //        .drop("Question")
    //        .drop("AnswerText")
    //        .drop("TimeAnswered")
    //      .groupBy("participantUUID","assessmentNumber","geotagStart","geotagEnd")
    //      .sum(questions: _*)
    //
    //    result.show()

    //val df3 = df2.withColumn("Q_id_string_cleaned", regexp_replace(df2("Q_id_string"), "\\'", ""))

    //val df4 = df3.withColumn("Q_id_string_new_2", regexp_replace(df3("Q_id_string_new"), "\\ ","_"))

    //df3.printSchema()

}


