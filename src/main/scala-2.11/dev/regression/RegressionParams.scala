package dev.regression

/**
  * Created by lucieburgess on 04/12/2017.
  * Class which specifies default values of parameters in the linear regression model.
  */
case class RegressionParams(elasticNetParam: Double = 0.0,
                            fitIntercept: Boolean = true,
                            maxIter: Int = 100,
                            regParam: Double = 0.0,
                            standardization: Boolean = false,
                            tol: Double = 1E-6,
                            fracTest: Double = 0.5,
                            input: String = "",
                            output: String = "")

