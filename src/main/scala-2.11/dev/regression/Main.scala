package dev.regression

import org.apache.log4j.{Level, Logger}

/**
  * Created by lucieburgess on 04/12/2017.
  * Main method for running the regression class.
  */
object Main {

  /** Switch to INFO if more detail is required but the output is verbose */

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    /**
      * Define parameters for regression model using org.apache.spark.ml.regression
      */

    val defaultParams = RegressionParams()

    val parser = new scopt.OptionParser[RegressionParams]("Regression test with different parameters using the Urban Mind pilot data") {

      head("Linear Regression parameters")

      opt[Double]("Elastic Net Parameter")
        .text(s"Elastic Net parameter, default: ${defaultParams.elasticNetParam}")
        .action((x, c) => c.copy(elasticNetParam = x))

      opt[Boolean]("Fit Intercept")
        .text(s"Fit intercept parameter, default: ${defaultParams.fitIntercept}")
        .action((x, c) => c.copy(fitIntercept = x))

      opt[Int]("Max Iterations")
        .text(s"Maximum number of iterations for optimisation, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))

      opt[Double]("Regularization parameter")
        .text(s"Regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))

      opt[Double]("Standardisation")
        .text(s"Standardisation parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))

    }

    /** Parameters for regression in Spark:
      * elasticNetParam: DoubleParam
      * fitIntercept: BooleanParam
      * maxIter: IntParam
      * regParam: DoubleParam
      * standardization: BooleanParam
      * tol: DoubleParam
      */




  }


}
