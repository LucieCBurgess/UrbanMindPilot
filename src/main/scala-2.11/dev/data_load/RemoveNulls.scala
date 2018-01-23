package dev.data_load

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lucieburgess on 23/01/2018.
  */
object RemoveNulls {

  def runRemoveNulls(): DataFrame = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Data cleaning object").getOrCreate()

    import spark.implicits._

    val inputpath = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/outputfullfiltered.csv"

    /** Load dataframe from csv file */
    val df = Csvload.createDataFrame(inputpath) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    /** Writes dataframe to a single csv file using the given path */
    def writeDFtoCsv(df: DataFrame, outputPath: String): Unit = {
      df.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputPath)
    }

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

    /** Reproduce sample sizes for participants who completed > 50% assessments */

    println("Number of momentary assessments indoors")
    val result = momDF.select(count(when($"104_Are you indoors or outdoors" === "Indoors", true))).show

    println("Number of momentary assessments outdoors")
    val result2 = momDF.select(count(when($"104_Are you indoors or outdoors" === "Outdoors", true))).show

    println("Can you see trees?")
    val result3 = momDF.select(count(when($"107_Can you see trees" === "yes", true))).show//968
    val result4 = momDF.select(count(when($"107_Can you see trees" === "no", true))).show//781
    val result5 = momDF.select(count(when($"107_Can you see trees" === "not sure", true))).show//26
    val result6 = momDF.select(count(when($"112_Can you see trees" === "yes", true))).show//245
    val result7 = momDF.select(count(when($"112_Can you see trees" === "no", true))).show//19
    val result8 = momDF.select(count(when($"112_Can you see trees" === "not sure", true))).show //3

    //FIXME need to continue this for the other measurements

    //FIXME fix nulls in baseDF for questions 028 and 029 - not used as predictors so not a priority

    /** Prepare baseline assessment file for regression - confounding variables */
    val calculateScoreMom: UserDefinedFunction = udf((columnName: String, answerText: String) => (columnName, answerText) match {

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

    val columnNames = Seq("002_Gender", "003_Where did you grow up", "005_What is your level of education", "006_Occupation",
      "007_How would you rate your physical health overall", "008_How would you rate your mental health overall")

    /** Adds a new column with numeric data for each of the base columns that we want to use as predictors */
    val baseDF2: DataFrame = columnNames.foldLeft(baseDF)(
      (baseDF, c) =>
        baseDF.withColumn(c.concat("_numeric"), calculateScoreBase(lit(c), baseDF(c)))
    )

    val output = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/baseDFnumeric.csv"
    writeDFtoCsv(baseDF2, output)

    val baseDF3 = baseDF2.drop("assessmentNumber")

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



  // don't forget to a return a dataframe

  }
}
