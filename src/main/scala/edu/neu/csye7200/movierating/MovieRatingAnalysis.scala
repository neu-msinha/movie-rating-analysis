package edu.neu.csye7200.movierating

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Try

/**
 * Movie Rating Analysis using Apache Spark
 *
 * This object provides pure functions to read movie data from CSV files
 * and calculate statistical measures (mean and standard deviation)
 * for IMDB scores.
 *
 * Dataset source: Kaggle IMDB 5000 Movie Dataset
 * https://www.kaggle.com/datasets/carolzhangdc/imdb-5000-movie-dataset
 */
object MovieRatingAnalysis {

  /**
   * Configuration case class for SparkSession creation (immutable)
   */
  case class SparkConfig(
                          appName: String = "MovieRatingAnalysis",
                          master: String = "local[*]",
                          bindAddress: String = "127.0.0.1"
                        )

  /**
   * Configuration case class for CSV reading options (immutable)
   */
  case class CsvOptions(
                         header: Boolean = true,
                         inferSchema: Boolean = true,
                         mode: String = "DROPMALFORMED"
                       )

  /**
   * Creates a SparkSession wrapped in Try for safe error handling
   *
   * @param config SparkConfig instance with configuration parameters
   * @return Try[SparkSession] - Success with session or Failure with exception
   */
  def createSparkSession(config: SparkConfig = SparkConfig()): Try[SparkSession] = Try {
    SparkSession.builder()
      .appName(config.appName)
      .master(config.master)
      .config("spark.driver.bindAddress", config.bindAddress)
      .getOrCreate()
  }

  /**
   * Reads a CSV file into a DataFrame using Spark's CSV data source
   * Pure function - returns Try for error handling
   *
   * @param spark  SparkSession instance
   * @param filePath Path to the CSV file
   * @param options  CsvOptions for reading configuration
   * @return Try[DataFrame] containing the CSV data or error
   */
  def readCsvFile(
                   spark: SparkSession,
                   filePath: String,
                   options: CsvOptions = CsvOptions()
                 ): Try[DataFrame] = Try {
    spark.read
      .option("header", options.header)
      .option("inferSchema", options.inferSchema)
      .option("mode", options.mode)
      .csv(filePath)
  }

  /**
   * Processes the movie DataFrame to calculate rating statistics
   * Pure function - DataFrame in, DataFrame out
   *
   * @param df Input DataFrame containing movie data with 'imdb_score' column
   * @return DataFrame with statistical measures
   */
  def calculateRatingStatistics(df: DataFrame): DataFrame =
    df.select(
      count("imdb_score").as("total_movies"),
      mean("imdb_score").as("mean_rating"),
      stddev_pop("imdb_score").as("stddev_population"),
      stddev_samp("imdb_score").as("stddev_sample"),
      min("imdb_score").as("min_rating"),
      max("imdb_score").as("max_rating")
    )

  /**
   * Extracts MovieStatistics from DataFrame safely
   * Pure function with Option return type for null safety
   *
   * @param df Input DataFrame (should be result of calculateRatingStatistics)
   * @return Option[MovieStatistics] - Some if successful, None if empty
   */
  def extractStatistics(df: DataFrame): Option[MovieStatistics] =
    df.collect().headOption.map { row =>
      MovieStatistics(
        totalMovies = row.getAs[Long]("total_movies"),
        meanRating = row.getAs[Double]("mean_rating"),
        stdDevPopulation = row.getAs[Double]("stddev_population"),
        stdDevSample = row.getAs[Double]("stddev_sample"),
        minRating = row.getAs[Double]("min_rating"),
        maxRating = row.getAs[Double]("max_rating")
      )
    }

  /**
   * Composes statistics calculation and extraction
   * Pure function using function composition
   *
   * @param df Input DataFrame containing movie data
   * @return Option[MovieStatistics]
   */
  def getStatistics: DataFrame => Option[MovieStatistics] =
    calculateRatingStatistics _ andThen extractStatistics

  /**
   * Filters DataFrame to include only valid ratings
   * Pure function - DataFrame in, DataFrame out
   *
   * @param df Input DataFrame
   * @return DataFrame with null/invalid ratings removed
   */
  def filterValidRatings(df: DataFrame): DataFrame =
    df.filter(col("imdb_score").isNotNull)
      .filter(col("imdb_score") > 0)

  /**
   * Categorizes a rating into a bucket
   * Pure function for rating classification
   */
  val ratingBucket: Double => String = rating =>
    if (rating < 4) "Poor (< 4)"
    else if (rating < 6) "Average (4-6)"
    else if (rating < 8) "Good (6-8)"
    else "Excellent (8+)"

  /**
   * Gets rating distribution by score ranges
   * Pure function - DataFrame in, DataFrame out
   *
   * @param df Input DataFrame
   * @return DataFrame with rating distribution
   */
  def getRatingDistribution(df: DataFrame): DataFrame =
    df.withColumn("rating_bucket",
        when(col("imdb_score") < 4, "Poor (< 4)")
          .when(col("imdb_score") < 6, "Average (4-6)")
          .when(col("imdb_score") < 8, "Good (6-8)")
          .otherwise("Excellent (8+)")
      )
      .groupBy("rating_bucket")
      .agg(count("*").as("count"))
      .orderBy("rating_bucket")

  /**
   * Complete analysis pipeline using function composition
   * Pure function that chains: filter -> calculate -> extract
   *
   * @param df Input DataFrame
   * @return Option[MovieStatistics]
   */
  def analysisPipeline(df: DataFrame): Option[MovieStatistics] =
    (filterValidRatings _ andThen getStatistics)(df)

  /**
   *
   * @param config SparkConfig
   * @param f      Function to execute with SparkSession
   * @tparam A     Return type
   * @return Try[A] result of the function
   */
  def withSparkSession[A](config: SparkConfig = SparkConfig())(f: SparkSession => A): Try[A] =
    createSparkSession(config).map { spark =>
      try {
        f(spark)
      } finally {
        spark.stop()
      }
    }
}

/**
 * Immutable case class to hold movie statistics
 */
case class MovieStatistics(
                            totalMovies: Long,
                            meanRating: Double,
                            stdDevPopulation: Double,
                            stdDevSample: Double,
                            minRating: Double,
                            maxRating: Double
                          ) {
  /**
   * Formatted string representation
   */
  def formatted: String =
    f"""
       |========================================
       |      Movie Rating Statistics
       |========================================
       | Total Movies Analyzed: $totalMovies
       | Mean IMDB Score:       $meanRating%.4f
       | Std Dev (Population):  $stdDevPopulation%.4f
       | Std Dev (Sample):      $stdDevSample%.4f
       | Min Rating:            $minRating%.1f
       | Max Rating:            $maxRating%.1f
       |========================================
     """.stripMargin

  override def toString: String = formatted
}