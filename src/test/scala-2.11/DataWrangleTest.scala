import dev.data_load.{Csvload, ScoreMap}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSuite

/**
  * @author lucieburgess on 08/12/2017
  *  Tests of the data pre-processing functions which clean and pivot the raw data file so it's in a suitable format
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

  /**
    * Add a column Q_id_string which concatenates the Q_id and the Question
    * So e.g. we have a column which contains entries such as 10_Please tell us what time you get up in the morning:
    * These entries will become column headings in the new data file used for analytics
    */
  val df2 = df.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
    .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("QuestionId"))

  /**
    * Helper method to add a Q_id_string column, tested in test [04] below
    */
  def addQIDStringColumn(inputDF: DataFrame): DataFrame = {
    val resultDF = inputDF.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("QuestionId"))
    resultDF
  }

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }

  val outputpath: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/raw_ordered_test.csv"

  /** -------------------------------- Tests from here ------------------------------------ */

  /** Tested by inspection of the output file */
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

  /**
    * Add a column Q_id_string which concatenates the Q_id and the Question
    * So e.g. we have a column which contains entries such as 10_Please tell us what time you get up in the morning:
    * These entries will become column headings in the new data file used for analytics
    */
  test("[04] Adding a Q_id_string column concatenates the QuestionId and the Question columns") {

    val df2 = df.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("QuestionId"))

    df2.printSchema()
    df2.show()

    assertResult(10) {
      df2.columns.length
    }

    /** 110 distinct Q_id_strings including Games questions */
    assertResult(110){
      df2.select($"Q_id_string").distinct.collect.length
    }

    df2.createOrReplaceTempView("datatable")

    val result = spark
      .sql("""SELECT Q_id_string
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=0
            AND QuestionId = 53""")

    assertResult("53_I've been interested in new things.") {
      result.collect.head.getString(0)
    }

    val result2 = spark
      .sql("""SELECT Q_id_string
            FROM datatable
            WHERE participantUUID='02E30B95-B9E6-49B4-BFB6-1719D48F14B3_1465478322'
            AND assessmentNumber=1
            AND QuestionId = 102""")

    assertResult("102_What are you doing right now?") {
      result2.collect.head.getString(0)
    }
  }

  /**
    * Filter out QuestionId relating to games as they are not used for analytics
    */
  test("[05] Filter out Q_ids relating to games") {

    val df3 = df.filter($"QuestionId" <2000)
    assertResult(480-60) {
      df3.count()
    }

    assertResult(105) {
      df3.select($"QuestionId").distinct.collect.length
    }
  }

  /**
    * ValidatedResponse is a new column of type String which combines AnswerText and AnswerValue.
    * AnswerText is a column which gives the raw text response provided by the participant.
    * AnswerValue converts some of the text answers to a numeric score for suitable questions, e.g. the Edinburgh-Warwick mental health
    * questions (41-54). However this seems inconsistent, e.g. 1_Age is an integer response and includes a string in the AnswerText field,
    * but 'None' in AnswerValue. Furthermore the ImpulsivityScore is calculated incorrectly.
    * So this test works but does not produce reliable results as ValidatedResponse doesn't contain correct answers.
    * The only solution is to calculate correct answers from the AnswerText column, as AnswerValue is not reliable.
    */
  test ("[06] Add a new column ValidatedResponse of StringType which combines the string and numeric answers, and check with SQL queries") {

    val df2 = addQIDStringColumn(df)

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

  /**
    * First attempt to create a pivoted dataframe - not used, but does create the desired result
    */
  test("[07] Create an array string from the Q_id_string column and use it to pivot dataframe") {

    val df2 = addQIDStringColumn(df)

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

    val df3: DataFrame = questions.foldLeft(df2) { //produces a DataFrame with each response in a column to the right, column header = question
      case (data, question) =>
        data.withColumn(question, when($"Q_id_string" === question, $"AnswerText"))
    }

    df3.printSchema()
    df3.show()

    val df4: DataFrame = df3
      .drop("QuestionId")
      .drop("Question")
      .drop("TimeAnswered")
      .groupBy("participantUUID","assessmentNumber","geotagStart","geotagEnd")
      .pivot("Q_id_string",questions) // .sum(questions: _*) doesn't work as questions is not numeric. Aggregate function must be numeric
      .agg(first($"AnswerText"))
      .orderBy("participantUUID", "assessmentNumber")

    df4.show()
    val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/questionsarray_test.csv"
    writeDFtoCsv(df4, output)
    df4.printSchema()
  }

  /**
    * Pivot the dataframe without creating the questions array
    */
  test("[08] Pivot dataframe using Q_id_string and re-order it in order of Question") {

    val df2 = addQIDStringColumn(df)

    val df3 = df2.withColumn("ValidatedResponse", when($"AnswerValue".startsWith("None"), $"AnswerText").otherwise($"AnswerValue"))

    val df4 = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd")
      .pivot("Q_id_string") // no need to also pivot using questions array
      .agg(first("ValidatedResponse")) //solves the aggregate function must be numeric problem
      .orderBy("participantUUID", "assessmentNumber")

    assertResult(114) {
      df4.columns.length
    }

    val numberOfRows: Int = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd").count().distinct().collect().length

    println(s"*********** Number of rows in the pivoted dataset is $numberOfRows ************")

    assert(df4.count() === numberOfRows)

  }

  /**
    * Add leading zeros to the QuestionId, which enables column headers of Q_id_string to be sorted in ascending order
    */
  test("[09] Add leading zeros to the QuestionId") {

    val addLeadingZeros: (Int => String) = s => "%03d".format(s)

    val newCol = udf(addLeadingZeros).apply(col("QuestionId")) // creates the new column
    val df3 = df.withColumn("Q_id_new", newCol) // adds the new column to original

    df3.printSchema()
    df3.show
  }

  /**
    * This is a simpler version so use this method instead.
    * https://stackoverflow.com/questions/8131291/how-to-convert-an-int-to-a-string-of-a-given-length-with-leading-zeros-to-align
    */
  test("[09B] Add leading zeros to the QuestionId using different udf method") {

    val addLeadingZeros = udf((number: Int) => {f"$number%03d"})

    val df3 = df.withColumn("Q_id_new", addLeadingZeros(df("QuestionId"))) // adds the new column to original

    df3.printSchema()
    df3.show
  }

  /** Select certain columns in the DataFrame in the order we want by placing them in an array */
  test("[10] Create a new DataFrame with the columns we want and cache it") {

    val df2 = addQIDStringColumn(df)
    val columnNames = df2.columns
    val reorderedColumnNames: Array[String] = Array("participantUUID","assessmentNumber","Q_id_string","AnswerText","AnswerValue","geotagStart","geoTagEnd")
    val result: DataFrame = df2.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    result.printSchema()
  }

  /** Delete "'", "," ":" and "?" from SQL column names */
  test("[11] Deal with unusual characters in SQL strings") {

    val df2 = addQIDStringColumn(df)
    val df3 = df2.withColumn("Q_id_string_cleaned", regexp_replace(df2.col("Q_id_string"),"[\\',\\?,\\,,\\:,\\.]",""))
    df3.printSchema()

    val result: String = df3.select($"Q_id_string_cleaned").as[String].collect().mkString("")
    def containsNoSpecialChars(string: String) = string.matches("^[a-zA-Z0-9][^'?,:.]*$")
    assertResult(true){
      containsNoSpecialChars(result)
    }
  }

  /**
    * Calculate the AnswerValue for the impulsivity score questions, which seems to be wrong
    * First match question number. If 31-35 it's an impulsivity question. If 36-39 it's a second set of impulsivity questions
    * and then map the answer text to a value. If it's an impulsivity question (31-39), calculate the correct score and put it in a new
    * column, "ImpulseAnswerValue". Otherwise, map it to a score of zero (or null).
    */
  test("[12] Calculate impulsivity score for the impulsivity score questions") {

    def calculateScore = udf((questionId: Int, answerText: String) => (questionId, answerText) match {

      case ((31 | 32 | 33 | 34 | 35), "Rarely /<br>Never") => 4
      case ((31 | 32 | 33 | 34 | 35), "Occasionally") => 3
      case ((31 | 32 | 33 | 34 | 35), "Often") => 2
      case ((31 | 32 | 33 | 34 | 35), "Almost always /<br>Always") => 1
      case ((36 | 37 | 38 | 39), "Rarely /<br>Never") => 1
      case ((36 | 37 | 38 | 39), "Occasionally") => 2
      case ((36 | 37 | 38 | 39), "Often") => 3
      case ((36 | 37 | 38 | 39), "Almost always /<br>Always") => 4
      case _ => 0
    })

    val df3 = df.withColumn("ImpulseAnswerValue", calculateScore(df("QuestionId"), df("AnswerText")))

    df3.printSchema()
    val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/impulsivity_test.csv"
    writeDFtoCsv(df3,output)
    df3.select("QuestionId","AnswerText","AnswerValue","ImpulseAnswerValue").show(50)
  }

  /**
    * Now check everything works for the test file in the following order:
    * Load the DF
    * Filter out game responses as not relevant
    * Add leading zeros to QuestionId
    * Concatenate QuestionId and Question with leading zeros to give Q_id_string e.g. 001_Age, 015_Do you meander and daydream as you walk?
    * Order by participantUUID,assessmentNumber,Q_id_string
    * Combine AnswerText and AnswerValue to get ValidatedResponse
    * Pivot dataframe using Q_id_string
    * Re-order it in order of: ParticipantUUID, assessmentNumber, Q_id_string, GeoTagStart, GeoTagEnd, Questions in ascending order
    * Finally re-set the schema of each column so that it corresponds to the correct type e.g. 1-2 times should be a string, 8am should be
    */
  test("[14] Putting it all together") {

    val addLeadingZeros = udf((number: Int) => {f"$number%03d"})

    val df2 = df.filter($"QuestionId" <2000)
      .withColumn("Q_id_new", addLeadingZeros(df("QuestionId")))
      .withColumn("Q_id_string", concat($"Q_id_new", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("Q_id_string"))
      .withColumn("ValidatedResponse", when($"AnswerValue".startsWith("None"), $"AnswerText").otherwise($"AnswerValue"))

    val df3 = df2.withColumn("Q_id_string_cleaned", regexp_replace(df2.col("Q_id_string"),"[\\',\\?,\\,,\\:,\\.]",""))

    val columnNames: Array[String] = df3.select($"Q_id_string_cleaned").distinct.as[String].collect.sortWith(_<_)

    assert(columnNames.length === 105)

    val df4 = df3.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd")
      .pivot("Q_id_string_cleaned") // and pivot by question?
      .agg(first("ValidatedResponse")) //solves the aggregate function must be numeric problem
      .orderBy("participantUUID", "assessmentNumber")

    val reorderedColumnNames: Array[String] = Array("participantUUID","assessmentNumber","geotagStart","geoTagEnd") ++ columnNames

    assert(reorderedColumnNames.length === 109)

    println("************** "+reorderedColumnNames.mkString(",")+" ***********")

    val df5: DataFrame = df4.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    val numberOfRows: Int = df2.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd").count().distinct().collect().length

    assert(df5.count === numberOfRows)
    assert(df5.columns.length === reorderedColumnNames.length)

  }
}


