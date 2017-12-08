package dev.regression

import org.apache.log4j.{Level, Logger}

/**
  * Created by lucieburgess on 04/12/2017.
  * Main method for running the regression class.
  */
object Main {

  /** Switch to INFO if more detail is required but the output is verbose */

  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

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

      opt[Boolean]("Standardization")
        .text(s"Standardisation parameter, default: ${defaultParams.standardParam}")
        .action((x, c) => c.copy(standardParam = x))

      opt[Double]("Tolerance")
        .text(s"Tolerance parameter, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))

      opt[Double]("Fraction for Test Data")
        .text(s"Fraction of data to be held out for testing, default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))

      opt[String]('i', "input")
        .text(s"input is the input path, default: ${defaultParams.input}")
        .action {(x, c) => c.copy(input = x)}

//      opt[String]('o', "output")
//        .text("output is the output path")
//        .required() action {(x, c) => c.copy(output = x)}

      checkConfig { params =>
        if (params.fracTest < 0.1 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value is incorrect; it should be in range [0.1,1).")
        } else {success}
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => DataWrangle.run(params)
      case _ => sys.exit(1)
    }
  }
}
