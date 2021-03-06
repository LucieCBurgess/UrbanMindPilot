package dev.data_load

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lucieburgess on 23/01/2018.
  * Removes null values from the dataframe produced by DataWrangle, and returns a new dataframe with only the columns
  * used as predictors.
  * val inputpath = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/outputfullfiltered.csv"
  */
object RemoveNulls {

  def runRemoveNulls(df: DataFrame): DataFrame = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Remove nulls object").getOrCreate()

    import spark.implicits._

    df.printSchema()
    println(s"Filtered pivoted dataset contains ${df.count()} rows")
    println(s"Filtered pivoted dataset contains ${df.columns.length} columns")

    /** Separate the dataset into baseline and momentary assessments */

    /** filters for baseline data, selects all baseline columns and checks schema */
    val df2 = df.filter($"assessmentNumber" === 0)
    val nRowsBase = df2.count().toInt
    println(s"The number of baseline assessments is $nRowsBase")

    val baseDF = df2.select((Array("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd", "baseWellBeingScore", "baseImpulseScore") ++ df2.columns.filter(_.startsWith("0"))).map(df2(_)): _*)
    baseDF.printSchema()

    /** filters for momentary data, selects all momentary assessments and checks schema */
    val df3 = df.filter($"assessmentNumber" > 0)
    val nRowsMom = df3.count().toInt
    println(s"The number of momentary assessments is $nRowsMom")

    val momDF = df3.select((Array("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd", "momWellBeingScore", "momImpulseScore") ++ df3.columns.filter(_.startsWith("1"))).map(df3(_)): _*)
    momDF.printSchema()

    //FIXME fix nulls in baseDF for questions 028 and 029 - not used as predictors so not a priority

    /** Prepare baseline assessment file for regression - confounding variables */
    val calculateScoreBase: UserDefinedFunction = udf((columnName: String, answerText: String) => (columnName, answerText) match {

      case ("002_Gender", "Female") => 0
      case ("002_Gender", "Male") => 1
      case ("002_Gender", "Other") => 2

      case ("003_Where did you grow up", "In the country") => 0 //PDF of questions states 'in a city' but this was an error, so 'In the country' and 'In a village' scores are merged
      case ("003_Where did you grow up", "In a village") => 0 //Q3 Where did you grow up?
      case ("003_Where did you grow up", "In a town") => 1
      case ("003_Where did you grow up", "Multiple places") => 2

      case ("005_What is your level of education", "Secondary school") => 0
      case ("005_What is your level of education", "Training college") => 1
      case ("005_What is your level of education", "Apprenticeship") => 2
      case ("005_What is your level of education", "University") => 3
      case ("005_What is your level of education", "Doctoral degree") => 4

      case ("006_Occupation", "Student") => 0
      case ("006_Occupation", "Employed") => 1
      case ("006_Occupation", "Self-employed") => 2
      case ("006_Occupation", "Retired") => 3
      case ("006_Occupation", "Unemployed") => 4

      case ("007_How would you rate your physical health overall", "Very poor") => 0
      case ("007_How would you rate your physical health overall", "Poor") => 1
      case ("007_How would you rate your physical health overall", "Fair") => 2
      case ("007_How would you rate your physical health overall", "Good") => 3
      case ("007_How would you rate your physical health overall", "Very good") => 4

      case ("008_How would you rate your mental health overall", "Very poor") => 0
      case ("008_How would you rate your mental health overall", "Poor") => 1
      case ("008_How would you rate your mental health overall", "Fair") => 2
      case ("008_How would you rate your mental health overall", "Good") => 3
      case ("008_How would you rate your mental health overall", "Very good") => 4

      case _ => -1
    })

    val columnNamesBase = Seq("002_Gender", "003_Where did you grow up", "005_What is your level of education", "006_Occupation",
      "007_How would you rate your physical health overall", "008_How would you rate your mental health overall")

    /** Adds a new column with numeric data for each of the base columns that we want to use as predictors */
    val baseDF2: DataFrame = columnNamesBase.foldLeft(baseDF)(
      (baseDF, c) =>
        baseDF.withColumn(c.concat("_numeric"), calculateScoreBase(lit(c), baseDF(c)))
    )

    val baseDF3 = baseDF2.select($"participantUUID", $"baseWellBeingScore", $"baseImpulseScore",	$"001_Age",	$"002_Gender",$"003_Where did you grow up",
    $"004_How many years have you lived in the city", $"005_What is your level of education", $"006_Occupation", $"007_How would you rate your physical health overall",
    $"008_How would you rate your mental health overall",$"002_Gender_numeric",	$"003_Where did you grow up_numeric",	$"005_What is your level of education_numeric",
    $"006_Occupation_numeric",	$"007_How would you rate your physical health overall_numeric",	$"008_How would you rate your mental health overall_numeric")

    println("Printing the schema of baseDF3")
    baseDF3.printSchema()

    /**
      * Now do the same for the momentary data.
      * Combine the indoors/ outdoors columns to get a single set of predictors, columns 201-205
      * Map the categorical string variables to numeric data and choose the predictors we want
      */
    val momDF2 = momDF.withColumn("201_Can you see trees", when($"107_Can you see trees".isNotNull,$"107_Can you see trees").otherwise($"112_Can you see trees")) //Indoors and Outdoors
      .withColumn("202_Can you see the sky",when($"108_Can you see the sky".isNotNull,$"108_Can you see the sky").otherwise("NA")) // if Outdoors, no data
      .withColumn("203_Can you hear birds singing", when($"113_Can you hear birds singing".isNotNull,$"113_Can you hear birds singing").otherwise("NA")) // if Indoors, no data
      .withColumn("204_Can you see or hear water", when($"114_Can you see or hear water".isNotNull, $"114_Can you see or hear water").otherwise("NA")) // If Indoors, no data
      .withColumn("205_Do you feel in contact with nature", when($"121_Do you feel you have any contact with nature in this place".isNotNull,$"121_Do you feel you have any contact with nature in this place").otherwise("NA"))

    /** Prepare momentary assessment file for regression - combine indoor and outdoor measurements */
    val calculateScoreMom: UserDefinedFunction = udf((columnName: String, answerText: String) => (columnName, answerText) match {

      case ("104_Are you indoors or outdoors", "Indoors") => 0
      case ("104_Are you indoors or outdoors", "Outdoors") => 1

      // Indoors or outdoors
      case ("201_Can you see trees", "no") => 0
      case ("201_Can you see trees", "not sure") => 0
      case ("201_Can you see trees", "yes") => 1

      // Indoors only
      case ("202_Can you see the sky", "no") => 0
      case ("202_Can you see the sky", "not sure") => 0
      case ("202_Can you see the sky", "yes") => 1

      // Outdoors only
      case ("203_Can you hear birds singing", "no") => 0
      case ("203_Can you hear birds singing", "not sure") => 0
      case ("203_Can you hear birds singing", "yes") => 1

      // Outdoors only
      case ("204_Can you see or hear water", "no") => 0
      case ("204_Can you see or hear water", "not sure") => 0
      case ("204_Can you see or hear water", "yes") => 1

      // Outdoors only
      case ("205_Do you feel in contact with nature", "no") => 0
      case ("205_Do you feel in contact with nature", "not sure") => 0
      case ("205_Do you feel in contact with nature", "yes") => 1

      case _ => -1
    })

    val columnNamesMom = Seq("104_Are you indoors or outdoors","201_Can you see trees", "202_Can you see the sky", "203_Can you hear birds singing", "204_Can you see or hear water", "205_Do you feel in contact with nature")

    /** Adds a new column with numeric data for each of the momentary columns that we want to use as predictors */
    val momDF3: DataFrame = columnNamesMom.foldLeft(momDF2)(
      (momDF2, c) =>
        momDF2.withColumn(c.concat("_numeric"), calculateScoreMom(lit(c), momDF2(c)))
    )

    val momDF4 = momDF3.select($"participantUUID".alias("momParticipantUUID"), $"assessmentNumber".alias("momAssessmentNumber"),$"geotagStart".alias("momGeoTagStart"),
      $"geotagEnd".alias("momGeoTagEnd"), $"momWellBeingScore", $"104_Are you indoors or outdoors", $"201_Can you see trees",	$"202_Can you see the sky",	$"203_Can you hear birds singing",
      $"204_Can you see or hear water", $"205_Do you feel in contact with nature", $"104_Are you indoors or outdoors_numeric", $"201_Can you see trees_numeric", $"202_Can you see the sky_numeric",
      $"203_Can you hear birds singing_numeric",$"204_Can you see or hear water_numeric",	$"205_Do you feel in contact with nature_numeric")

    println("Printing the schema of momDF4")
    momDF4.printSchema()

    /** Finally join the two datasets to get a dataset without nulls and select relevant columns */

    val joinedDF = momDF4.join(baseDF3, momDF4("momParticipantUUID") === baseDF3("participantUUID"))

    println("Printing the schema of joinedDF")
    joinedDF.printSchema()

    val outputJoined = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/joinedDFnumeric.csv"
    writeDFtoCsv(joinedDF, outputJoined)

    joinedDF

  }

  // ******************** Helper function *****************************

  /** Writes dataframe to a single csv file using the given path */
  def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputPath)
  }

}
