import dev.data_load.Csvload
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by lucieburgess on 19/01/2018.
  * This uses the output from the DataWrangle operation. The data is clean, ordered by column, each column is an attribute/predictor
  * Records for participants who answered less than 50% of assessments were removed
  * Now we separate the data into 2 DataFrames: one for baseline assessments, one for momentary assessments
  * Then we can perform a join which should copy the data across into cells where there are no responses
  * To obtain data, run DataWrangle and then use the output.
  */
class RemoveNullsTest extends FunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("RemoveNullsTest")
    .getOrCreate()

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
      .option("header","true")
      .save(outputPath)
  }

  /** -------------------------------- Tests from here ------------------------------------ */

  /** Tested by inspection of the output file */
  test("[01] Create a separate DF of baseline assessments only") {

    df.printSchema()

    val df2 = df.filter($"assessmentNumber" === 0)
    val nRowsBase = df2.count().toInt
    println(s"The number of baseline assessments is $nRowsBase")

    // selects all baseline assessments
    val baseDF = df2.select((Array("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd", "baseWellBeingScore", "baseImpulseScore") ++ df2.columns.filter(_.startsWith("0"))).map(df2(_)): _*)

    baseDF.printSchema()

    val df3 = df.filter($"assessmentNumber" > 0)
    val nRowsMom = df3.count().toInt
    println(s"The number of momentary assessments is $nRowsMom")

    // selects all momentary assessments
    val momDF = df3.select((Array("participantUUID", "assessmentNumber", "geotagStart", "geotagEnd", "momWellBeingScore", "momImpulseScore") ++ df3.columns.filter(_.startsWith("1"))).map(df3(_)): _*)

    momDF.printSchema()

    //Put lines back in if you need to write to file, otherwise remove to simply run the analysis once the files have been written for the first time

    //val outputBase = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/baseDF.csv"
    //writeDFtoCsv(baseDF, outputBase)

    //val outputMom = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/momDF.csv"
    //writeDFtoCsv(momDF, outputMom)

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
  }

  /** Calculate validated score as an int and add new column to the base/ mom files as necessary */
  test("[02] Convert String responses to numeric for ordinal data questions") {

    val calculateScore = udf((columnName: String, answerText: String) => (columnName, answerText) match {

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

      case ("007_How would you rate your physical health overall", "Poor") => 0
      case ("007_How would you rate your physical health overall", "Fair") => 1
      case ("007_How would you rate your physical health overall", "Good") => 2
      case ("007_How would you rate your physical health overall", "Very good") => 3

      case ("008_How would you rate your mental health overall", "Poor") => 0
      case ("008_How would you rate your mental health overall", "Fair") => 1
      case ("008_How would you rate your mental health overall", "Good") => 2
      case ("008_How would you rate your mental health overall", "Very good") => 3

      case _ => -1
    })

    val inputBase = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/baseDF.csv"

    /** Load dataframe from csv file */
    val baseDF = Csvload.createDataFrame(inputBase) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    val columnNames = Seq("001_Age", "002_Gender", "003_Where did you grow up", "005_What is your level of education", "006_Occupation",
      "007_How would you rate your physical health overall", "008_How would you rate your mental health overall")

    //val df2 = baseDF.select(columnNames.map(baseDF(_)): _*) //selects columns in the baseDF

    // adds each column name in columnNames to the DF. baseDF is the accumulator
    val newDF: DataFrame = columnNames.foldLeft[DataFrame](baseDF)(
      (acc, c) =>
        acc.withColumn(c, col(c))
    )

    // adds each column name in columnNames to the DF. baseDF is the accumulator
    // Adds a new column for each column in columnNames, then applies the udf CalculateScore row by row to the DF column, depending on the column name
    // FIXME this is not working - returns -1, so it's not picking up the column name or column contents correctly
    // see test [07] DataWrangleTest
    val newDF2: DataFrame = columnNames.foldLeft(baseDF)(
      (baseDF, c) =>
        baseDF.withColumn(c.concat("_numeric"), calculateScore(baseDF(c), baseDF(c))) //calculateScore(col(c), baseDF(col(c))
    )

    newDF2.select("participantUUID", "assessmentNumber", "002_Gender", "002_Gender_numeric", "003_Where did you grow up", "003_Where did you grow up_numeric").show()
  }

  //Reminder of pivot function for a StackOverflow question
  /** https://stackoverflow.com/questions/42643737/spark-applying-udf-to-dataframe-generating-new-columns-based-on-values-in-df */
  test("[03] playing with pivots") {

    val df = Seq(("A","X",6,-1),
      ("B", "Z",-1,5),
      ("C","Y",4,-1)).toDF("col1","col2","col3","col4")

    df.show() // this won't work with null values so I've used -1 instead

    val inputpath = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/simpletest.csv" //simpletest contains nulls

    /** Load dataframe from csv file */
    val df2 = Csvload.createDataFrame(inputpath) match {
      case Some(dfload) => dfload
      case None => throw new UnsupportedOperationException("Couldn't create DataFrame")
    }

    df2.show()

    // Step 1. Add a new column which contains the contents of col1 concatenated with col2
    // Step 2. // add a new column, "value" which contains the non-null contents of either col3 or col4
    // Step 3. GroupBy the columns you want
    // Step 4. pivot on newCol, which contains the values which are now to be column headings
    // Step 5. Aggregate by the max of value, which will be the value itself if the groupBy is single-valued per group
    // or alternatively .agg(first($"value") if value happens to be a string rather than a numeric type - max function can only be applied to a numeric type
    // Step 6. order by newCol so DF is in ascending order
    // Step 7. drop this column as you no longer need it, or skip this step if you want a column of values without nulls

    val df3 = df2.withColumn("newCol", concat($"col1", $"col2")) //Step 1
      .withColumn("value",when($"col3".isNotNull, $"col3").otherwise($"col4")) //Step 2
      .groupBy($"col1",$"col2",$"col3",$"col4",$"newCol") //Step 3
      .pivot("newCol") // Step 4
      .agg(max($"value")) // Step 5
      .orderBy($"newCol") // Step 6
      .drop($"newCol") // Step 7

      df3.show()
  }
}
