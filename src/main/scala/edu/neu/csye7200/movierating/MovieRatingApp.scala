package edu.neu.csye7200.movierating

import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}

/**
 * Main Application for Movie Rating Analysis
 *
 * Uses functional error handling with Try/Either and avoids mutable state.
 *
 * Dataset source: Kaggle IMDB 5000 Movie Dataset
 * https://www.kaggle.com/datasets/carolzhangdc/imdb-5000-movie-dataset
 */
object MovieRatingApp extends App {

  // Reduce Spark logging verbosity (side effect isolated at entry point)
  configureLogging()

  // Program configuration (immutable)
  val csvFilePath = "data/movie_metadata.csv"
  val sparkConfig = MovieRatingAnalysis.SparkConfig(appName = "MovieRatingAnalysis")

  // Execute the functional pipeline
  val result: Try[Unit] = runAnalysis(csvFilePath, sparkConfig)

  // Handle result (side effects at the edge)
  result match {
    case Success(_) => println("\nAnalysis completed successfully.")
    case Failure(e) => println(s"\nAnalysis failed: ${e.getMessage}")
  }

  /**
   * Configure logging - isolated side effect
   */
  def configureLogging(): Unit = {
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.WARN)
  }

  /**
   * Main analysis pipeline using functional composition
   * All side effects (printing) are isolated here at the application boundary
   *
   * @param filePath    Path to CSV file
   * @param sparkConfig Spark configuration
   * @return Try[Unit] indicating success or failure
   */
  def runAnalysis(filePath: String, sparkConfig: MovieRatingAnalysis.SparkConfig): Try[Unit] =
    MovieRatingAnalysis.withSparkSession(sparkConfig) { spark =>
      printHeader()

      // Functional pipeline with for-comprehension
      val analysisResult: Either[String, AnalysisResult] = for {
        moviesDF <- MovieRatingAnalysis.readCsvFile(spark, filePath).toEither.left.map(_.getMessage)
        _ = printSchemaInfo(moviesDF)
        _ = printSampleData(moviesDF)
        validMoviesDF = MovieRatingAnalysis.filterValidRatings(moviesDF)
        stats <- MovieRatingAnalysis.getStatistics(validMoviesDF).toRight("Failed to calculate statistics")
        distribution = MovieRatingAnalysis.getRatingDistribution(validMoviesDF)
      } yield AnalysisResult(stats, distribution, validMoviesDF)

      // Handle result
      analysisResult match {
        case Right(result) => printResults(result)
        case Left(error) => println(s"Error: $error")
      }
    }

  /**
   * Immutable result container
   */
  case class AnalysisResult(
                             statistics: MovieStatistics,
                             distribution: DataFrame,
                             validMovies: DataFrame
                           )

  // Pure functions for printing (side effects, but isolated at boundary)

  def printHeader(): Unit = {
    println("=" * 50)
    println("  Movie Rating Analysis - CSYE7200 Assignment")
    println("=" * 50)
  }

  def printSchemaInfo(df: DataFrame): Unit = {
    println("\nDataFrame Schema:")
    df.printSchema()
  }

  def printSampleData(df: DataFrame): Unit = {
    println("\nSample Data (first 5 rows):")
    df.select("movie_title", "director_name", "imdb_score", "title_year")
      .show(5, truncate = false)
  }

  def printResults(result: AnalysisResult): Unit = {
    println("\nCalculating rating statistics...")
    println(result.statistics)

    println("\nRating Distribution:")
    result.distribution.show()

    println("\nTop 10 Highest Rated Movies:")
    result.validMovies
      .select("movie_title", "director_name", "imdb_score", "title_year")
      .orderBy(org.apache.spark.sql.functions.desc("imdb_score"))
      .show(10, truncate = false)
  }
}