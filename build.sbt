name := "Urban_Mind_Pilot"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/")

// Scalatest dependencies
libraryDependencies ++= Seq("org.scalactic" %% "scalactic" % "3.0.1", "org.scalatest" %% "scalatest" % "3.0.1" % "test")

// Spark dependencies
lazy val sparkVersion = "2.2.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion, "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion, "org.apache.spark" %% "spark-mllib" % sparkVersion)

// Scopt dependencies
libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"



