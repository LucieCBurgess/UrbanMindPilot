package dev.regression

/**
  * Created by lucieburgess on 04/12/2017.
  * Class which specifies default values of parameters in the linear regression model.
  *
  * Regression parameters in Spark: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.regression.LinearRegression
  * elasticNetParam: ElasticNet mixing parametr in the range [0,1]
  * fitIntercept: Boolean - parameter for whether to fit an intercept term
  * maxIter: Int - parameter for the maximum number of iterations >=0
  * regParam: Double - paramer for regularization, >=0
  * standardization: Boolean - parameter for whether to standardize the training features before fitting the model
  * tol: Double - parameter for the convergence tolerance for iterative algorithms (>=0)
  *
  * Additional parameters:
  * fracTest: Double - fraction of the input data to be held out for testing
  * input: input path to the data file
  * output: output path to the file to be written containing the results.
  */

case class RegressionParams(elasticNetParam: Double = 0.0,
                            fitIntercept: Boolean = true,
                            maxIter: Int = 100,
                            regParam: Double = 0.0,
                            standardParam: Boolean = false,
                            tol: Double = 1E-6,
                            fracTest: Double = 0.5,
                            input: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/db_1489678713_raw.csv",
                            output: String = "/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/Pilot_data_output/output.txt")

