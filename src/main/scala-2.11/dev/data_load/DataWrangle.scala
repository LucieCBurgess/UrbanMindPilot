package dev.data_load

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lucieburgess on 05/01/2018.
  * This object loads the raw data csv file, cleans it and transforms it to a format that can be analysed.
  */
object DataWrangle {

  val output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/outputfullfiltered.csv"

  def runDataWrangle(inputFile: String): DataFrame = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Data cleaning object").getOrCreate()

    import spark.implicits._


    /** Load training and test data and cache it */
    val df = Csvload.createDataFrame(inputFile) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    df.printSchema()
    println(s"Raw dataset contains ${df.count()} rows")
    println(s"Raw dataset contains ${df.columns.length} columns")

    val addLeadingZeros = udf((number: Int) => {
      f"$number%03d"
    })

    /** Calculate validated score */
    val calculateScore = udf((questionId: Int, answerText: String) => (questionId, answerText) match {

      case (x, "Never") if 13 to 22 contains x => "1" //urban environment questions
      case (x, "Rarely") if 13 to 22 contains x => "2"
      case (x, "Sometimes") if 13 to 22 contains x => "3"
      case (x, "Often") if 13 to 22 contains x => "4"
      case (x, "Always") if 13 to 22 contains x => "5"

      case (x, "Rarely /<br>Never") if 31 to 35 contains x => "4" //Trait impulsivity - baseline
      case (x, "Occasionally") if 31 to 35 contains x => "3"
      case (x, "Often") if 31 to 35 contains x => "2"
      case (x, "Almost always /<br>Always") if 31 to 35 contains x => "1"

      case (x, "Rarely /<br>Never") if 36 to 39 contains x => "1" //Trait impulsivity - baseline
      case (x, "Occasionally") if 36 to 39 contains x => "2"
      case (x, "Often") if 36 to 39 contains x => "3"
      case (x, "Almost always /<br>Always") if 36 to 39 contains x => "4"

      case (x, "None of<br>the time") if 41 to 54 contains x => "1" //EW wellbeing questions - baseline
      case (x, "Rarely") if 41 to 54 contains x => "2"
      case (x, "Some of<br>the time") if 41 to 54 contains x => "3"
      case (x, "Often") if 41 to 54 contains x => "4"
      case (x, "All of<br>the time") if 41 to 54 contains x => "5"

      case (27, "Never") => "0" //alcohol consumption
      case (27, "Monthly or less") => "1"
      case (27, "2-4 times per month") => "2"
      case (27, "2-3 times per week") => "3"
      case (27, "4+ times per week") => "4"

      case (28 | 152, "1-2") => "0" //alcohol consumption
      case (28 | 152, "3-4") => "1"
      case (28 | 152, "5-6") => "2"
      case (28 | 152, "7-9") => "3"
      case (28 | 152, "10+") => "4"
      case (28 | 152, "01-Feb") => "0" // deals with problem where 1-2 parses as a date in the input csv file
      case (28 | 152, "03-Apr") => "1" //3-4
      case (28 | 152, "06-Jun") => "2" //5-6
      case (28 | 152, "07-Sep") => "3" //7-9

      case (29, "Never") => "0" //alcohol consumption
      case (29, "Less than monthly") => "1"
      case (29, "Monthly") => "2"
      case (29, "Weekly") => "3"
      case (29, "Daily or almost daily") => "4"

      case (x, "I very much disagree") if 131 to 144 contains x => "1" //EW wellbeing questions - momentary
      case (x, "I slightly disagree") if 131 to 144 contains x => "2"
      case (x, "Not sure") if 131 to 144 contains x => "3"
      case (x, "I slightly agree") if 131 to 144 contains x => "4"
      case (x, "I very much agree") if 131 to 144 contains x => "5"

      case (x, "I very much disagree") if 154 to 158 contains x => "5" //Trait impulsivity - momentary
      case (x, "I slightly disagree") if 154 to 158 contains x => "4"
      case (x, "Not sure") if 154 to 158 contains x => "3"
      case (x, "I slightly agree") if 154 to 158 contains x => "2"
      case (x, "I very much agree") if 154 to 158 contains x => "1"

      case (x, "I very much disagree") if 159 to 162 contains x => "1" //Trait impulsivity - momentary
      case (x, "I slightly disagree") if 159 to 162 contains x => "2"
      case (x, "Not sure") if 159 to 162 contains x => "3"
      case (x, "I slightly agree") if 159 to 162 contains x => "4"
      case (x, "I very much agree") if 159 to 162 contains x => "5"

      case _ => answerText
    })

    /**
      * Filter out question Ids relating to games
      * Create a new column, Q_id_string - each row will become a column in the new dataset
      * Calculate the correct scores from the answer text - particularly important for impulsivity as the database output was incorrect
      */
    val df2 = df.filter($"QuestionId" < 2000)
      .withColumn("Q_id_new", addLeadingZeros(df("QuestionId")))
      .withColumn("Q_id_string", concat($"Q_id_new", lit("_"), $"Question"))
      .orderBy(asc("participantUUID"), asc("assessmentNumber"), asc("Q_id_string"))
      .withColumn("ValidatedScore", calculateScore(df("QuestionId"), df("AnswerText")))

    /**
      *  Select the participant UUID and give it an alias, filtered participant UUID
      *  Count number of assessments per participant as 'totalAssessments'
      *  keep records only for which number of assessments > 25
      */
    val df3: DataFrame = df2.select($"participantUUID".alias("filteredParticipantUUID"),$"assessmentNumber")
      .groupBy("filteredParticipantUUID")
      .agg(countDistinct("assessmentNumber") as("totalAssessments"))
      .filter($"totalAssessments" > 25)

    df3.show()

    val df4 = df3.join(df2, df3("filteredParticipantUUID") === df2("participantUUID"))

    val df5 = df4.withColumn("Q_id_string_cleaned", regexp_replace(df4.col("Q_id_string"), "[\\',\\?,\\,,\\:,\\.]", ""))

    val columnNames: Array[String] = df5.select($"Q_id_string_cleaned").distinct.as[String].collect.sortWith(_ < _)

    val df6 = df5.groupBy("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd")
      .pivot("Q_id_string_cleaned")
      .agg(first("ValidatedScore"))
      .orderBy("participantUUID", "assessmentNumber")

    val reorderedColumnNames: Array[String] = Array("participantUUID", "assessmentNumber", "geotagStart", "geoTagEnd") ++ columnNames

    println("************** " + reorderedColumnNames.mkString(",") + " ***********")

    val df7: DataFrame = df6.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    println(s"Cleaned pivoted dataset contains ${df7.count()} rows")
    println(s"Cleaned pivoted dataset contains ${df7.columns.length} columns")

    /**
      * Now calculate the Edinburgh-Warwick wellbeing score at baseline
      */
    val baseWellBeingCols = List(
      col("041_Ive been feeling optimistic about the future"),
      col("042_Ive been feeling useful"),
      col("043_Ive been feeling relaxed"),
      col("044_Ive been feeling interested in other people"),
      col("045_Ive had energy to spare"),
      col("046_Ive been dealing with problems well"),
      col("047_Ive been thinking clearly"),
      col("048_Ive been feeling good about myself"),
      col("049_Ive been feeling close to other people"),
      col("050_Ive been feeling confident"),
      col("051_Ive been able to make up my own mind about things"),
      col("052_Ive been feeling loved"),
      col("053_Ive been interested in new things"),
      col("054_Ive been feeling cheerful"))

    /**
      * Now calculate the Edinburgh-Warwick wellbeing score during momentary assessments
      */
    val momWellBeingCols = List(
      col("131_Right now I feel optimistic about the future"),
      col("132_Right now I feel useful"),
      col("133_Right now I feel relaxed"),
      col("134_Right now I feel interested in other people"),
      //col("135_Right now I feel energy to spare"),
      col("135_Right now I have energy to spare"),
      col("136_Right now I deal with problems well"),
      col("137_Right now I think clearly"),
      col("138_Right now I feel good about myself"),
      col("139_Right now I feel close to other people"),
      col("140_Right now I feel confident"),
      col("141_Right now I am able to make up my own mind about things"),
      col("142_Right now I feel loved"),
      col("143_Right now I am interested in new things"),
      col("144_Right now I feel cheerful"))

    /**
      * Now calculate the Impulsivity score at baseline
      */
    val baseImpulseCols = List(
      col("031_I am focused seeing things through to the end"),
      col("032_I plan work tasks and activities in my free time carefully"),
      col("033_I plan events and activities well ahead of time"),
      col("034_I think carefully before doing and saying things"),
      col("035_I find it easy to exercise self-control"),
      col("036_I encounter problems because I do things without stopping to think"),
      col("037_I become involved with things that I later wish I could get out of"),
      col("038_I tend to jump from one interest to another"),
      col("039_I tend to act on impulse"))

    /**
      * Now calculate the Impulsivity score during momentary assessments
      */
    val momImpulseCols = List(
      col("154_Today I was focused seeing things through to the end"),
      col("155_Today I planned work tasks and activities in my free time carefully"),
      col("156_Today I planned events and activities well ahead of time"),
      col("157_Today I thought carefully before doing and saying things"),
      col("158_Today I found it easy to exercise self-control"),
      col("159_Today I encountered problems because I do things without stopping to think"),
      col("160_Today I became involved with things that I later wish I could get out of"),
      col("161_Today I jumped from one interest to another"),
      col("162_Today I acted on impulse"))

    val df8 = df7.withColumn("baseWellBeingScore", baseWellBeingCols.reduce(_ + _))
      .withColumn("momWellBeingScore", momWellBeingCols.reduce(_ + _))
      .withColumn("baseImpulseScore", baseImpulseCols.reduce(_ + _))
      .withColumn("momImpulseScore", momImpulseCols.reduce(_ + _))

    df8.select($"participantUUID", $"assessmentNumber", $"baseWellBeingScore", $"momWellBeingScore", $"baseImpulseScore", $"momImpulseScore").show()

    writeDFtoCsv(df8, output)
    df8.printSchema()

    /** Output is the full pivoted dataset, filtered for participants with >50% of assessments only. Written to file outputfullfiltered.csv */

    println(s"Filtered pivoted dataset contains ${df8.count()} rows")
    println(s"Filtered pivoted dataset contains ${df8.columns.length} columns")

    df8
  }



  /*********** Helper functions *******************/

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {

    df.coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header","true")
    .save(outputPath)
  }
}
