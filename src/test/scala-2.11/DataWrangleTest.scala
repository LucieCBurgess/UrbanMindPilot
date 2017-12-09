import dev.data_load.Csvload
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, concat, lit, regexp_replace, when}
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

  val df = Csvload.createDataFrame(inputpath) match {
    case Some(dfload) => dfload
    case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
  }

  val df2 = df.withColumn("Q_id_string", concat($"QuestionId", lit("_"), $"Question"))
    .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("QuestionId"))

  test("[01] Loading test dataframe creates a DF of the correct number of rows") {
    df.printSchema()
    assertResult(480) {
      df.count()
    }
  }

  test("[02] Loading test dataframe creates a DF of the correct number of columns") {
    assertResult(9) {
      df.columns.length
    }
  }

  test("[03] Adding a Q_id_string column concatenates the QuestionId and the Question columns") {
    df2.printSchema()
    assertResult(10) {
      df2.columns.length
    }
  }

  test("[04] Create an array string from the Q_id_string column") {

    import spark.implicits._

    //val df3 = df2.withColumn("Q_id_string_cleaned", regexp_replace(df2("Q_id_string"), "\\'", ""))

    //val df4 = df3.withColumn("Q_id_string_new_2", regexp_replace(df3("Q_id_string_new"), "\\ ","_"))

    //df3.printSchema()

    val questions: Array[String] = df2.select("Q_id_string")
      .distinct()
      .collect()
      .map(_.getAs[String]("Q_id_string"))
      .sortWith(_<_)

    assertResult(110) {
      questions.length
    }

    println(questions.mkString(","))

    assert(questions(0).equals("101_Who is with you right now?"))
    assert(questions(1).equals("102_What are you doing right now?"))
    assert(questions(questions.length-1).equals("8_How would you rate your mental health overall?"))

    val df3: DataFrame = questions.foldLeft(df2) {
      case (data, question) =>
        data.withColumn(question, when($"Q_id_string" === question, $"AnswerText"))
    }

    df3.printSchema()

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


  }
}


