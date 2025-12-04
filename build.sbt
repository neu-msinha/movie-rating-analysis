name := "movie-rating-analysis"

version := "1.0.0"

scalaVersion := "2.12.15"

// Spark 3.2.1 dependencies for Scala 2.12.x
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Testing dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Set main class
Compile / mainClass := Some("edu.neu.csye7200.movierating.MovieRatingApp")

// Avoid running tests in parallel
Test / parallelExecution := false

// Fork JVM for running
run / fork := true
Test / fork := true

// JVM options
val commonJavaOptions = Seq(
  "-Xms512M",
  "-Xmx2G",
  "--illegal-access=permit",  // Suppress illegal reflective access warnings
  "-Dlog4j.logLevel=ERROR"    // Reduce Spark logging verbosity
)

run / javaOptions ++= commonJavaOptions
Test / javaOptions ++= commonJavaOptions