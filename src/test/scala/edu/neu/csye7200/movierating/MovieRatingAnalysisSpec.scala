package edu.neu.csye7200.movierating

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.util.{Success, Try}

/**
 * Test Suite for MovieRatingAnalysis - Functional Implementation
 *
 * Tests use pure functions and immutable data where possible.
 */
class MovieRatingAnalysisSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Lazy initialization - evaluated only when needed (referentially transparent)
  private lazy val spark: SparkSession = createTestSparkSession.get

  // Test data schema (immutable)
  private val testSchema: StructType = StructType(Seq(
    StructField("movie_title", StringType, nullable = true),
    StructField("imdb_score", DoubleType, nullable = true)
  ))

  override def beforeAll(): Unit = {
    configureLogging()
    // Force lazy val evaluation
    spark
  }

  override def afterAll(): Unit = {
    Option(spark).foreach(_.stop())
  }

  /**
   * Pure function to configure logging
   */
  private def configureLogging(): Unit =
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)

  /**
   * Pure function to create SparkSession wrapped in Try
   */
  private def createTestSparkSession: Try[SparkSession] = Try {
    SparkSession.builder()
      .appName("MovieRatingAnalysisTest")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  /**
   * Pure function to create test DataFrame from sequence of tuples
   */
  private def createTestDataFrame(data: Seq[(String, Double)]): DataFrame = {
    val rows = data.map { case (title, score) => Row(title, score) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
  }

  /**
   * Pure function to create test DataFrame with nullable scores
   */
  private def createTestDataFrameWithNulls(data: Seq[(String, Option[Double])]): DataFrame = {
    val rows = data.map { case (title, scoreOpt) => Row(title, scoreOpt.orNull) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
  }

  // ==================== Tests for createSparkSession ====================

  "createSparkSession" should "return Success with valid SparkSession" in {
    val result = MovieRatingAnalysis.createSparkSession()
    result shouldBe a[Success[_]]
    result.foreach { session =>
      session should not be null
      session.sparkContext should not be null
    }
  }

  // ==================== Tests for readCsvFile ====================

  "readCsvFile" should "return Success with DataFrame for valid file" in {
    val result = MovieRatingAnalysis.readCsvFile(spark, "data/movie_metadata.csv")
    result shouldBe a[Success[_]]
    result.foreach(_.count() should be > 0L)
  }

  it should "return DataFrame with expected columns" in {
    val result = MovieRatingAnalysis.readCsvFile(spark, "data/movie_metadata.csv")
    result.foreach { df =>
      df.columns should contain("imdb_score")
      df.columns should contain("movie_title")
      df.columns should contain("director_name")
    }
  }

  it should "return Failure for non-existent file" in {
    val result = MovieRatingAnalysis.readCsvFile(spark, "non_existent.csv")
    result.isFailure shouldBe true
  }

  // ==================== Tests for calculateRatingStatistics ====================

  "calculateRatingStatistics" should "calculate correct mean for simple dataset" in {
    val testData = Seq(
      ("Movie A", 8.0),
      ("Movie B", 6.0),
      ("Movie C", 7.0),
      ("Movie D", 9.0),
      ("Movie E", 5.0)
    )
    val testDF = createTestDataFrame(testData)
    val statsDF = MovieRatingAnalysis.calculateRatingStatistics(testDF)
    val meanRating = statsDF.first().getAs[Double]("mean_rating")

    // Expected mean: (8 + 6 + 7 + 9 + 5) / 5 = 7.0
    meanRating shouldBe 7.0 +- 0.001
  }

  it should "calculate correct standard deviation" in {
    val testData = Seq(
      ("Movie A", 2.0),
      ("Movie B", 4.0),
      ("Movie C", 4.0),
      ("Movie D", 4.0),
      ("Movie E", 5.0),
      ("Movie F", 5.0),
      ("Movie G", 7.0),
      ("Movie H", 9.0)
    )
    val testDF = createTestDataFrame(testData)
    val statsDF = MovieRatingAnalysis.calculateRatingStatistics(testDF)
    val stdDevPop = statsDF.first().getAs[Double]("stddev_population")

    // Mean = 5.0, Variance = 4.0, StdDev = 2.0
    stdDevPop shouldBe 2.0 +- 0.001
  }

  it should "handle single value dataset" in {
    val testData = Seq(("Movie A", 7.5))
    val testDF = createTestDataFrame(testData)
    val statsDF = MovieRatingAnalysis.calculateRatingStatistics(testDF)
    val row = statsDF.first()

    row.getAs[Long]("total_movies") shouldBe 1
    row.getAs[Double]("mean_rating") shouldBe 7.5
    row.getAs[Double]("stddev_population") shouldBe 0.0
  }

  it should "return correct count" in {
    val testData = Seq(
      ("Movie A", 8.0),
      ("Movie B", 6.0),
      ("Movie C", 7.0)
    )
    val testDF = createTestDataFrame(testData)
    val statsDF = MovieRatingAnalysis.calculateRatingStatistics(testDF)

    statsDF.first().getAs[Long]("total_movies") shouldBe 3
  }

  // ==================== Tests for filterValidRatings ====================

  "filterValidRatings" should "remove null ratings" in {
    val testData = Seq(
      ("Movie A", Some(8.0)),
      ("Movie B", None),
      ("Movie C", Some(7.0))
    )
    val testDF = createTestDataFrameWithNulls(testData)
    val filtered = MovieRatingAnalysis.filterValidRatings(testDF)

    filtered.count() shouldBe 2
  }

  it should "remove zero or negative ratings" in {
    val testData = Seq(
      ("Movie A", 8.0),
      ("Movie B", 0.0),
      ("Movie C", -1.0),
      ("Movie D", 7.0)
    )
    val testDF = createTestDataFrame(testData)
    val filtered = MovieRatingAnalysis.filterValidRatings(testDF)

    filtered.count() shouldBe 2
  }

  // ==================== Tests for getStatistics (composed function) ====================

  "getStatistics" should "return Some with correct MovieStatistics" in {
    val testData = Seq(
      ("Movie A", 8.0),
      ("Movie B", 6.0),
      ("Movie C", 7.0),
      ("Movie D", 9.0),
      ("Movie E", 5.0)
    )
    val testDF = createTestDataFrame(testData)
    val statsOpt = MovieRatingAnalysis.getStatistics(testDF)

    statsOpt shouldBe defined
    statsOpt.foreach { stats =>
      stats.totalMovies shouldBe 5
      stats.meanRating shouldBe 7.0 +- 0.001
      stats.minRating shouldBe 5.0
      stats.maxRating shouldBe 9.0
    }
  }

  // ==================== Tests for extractStatistics ====================

  "extractStatistics" should "return None for empty DataFrame" in {
    val emptyDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      MovieRatingAnalysis.calculateRatingStatistics(createTestDataFrame(Seq(("A", 5.0)))).schema
    )
    val result = MovieRatingAnalysis.extractStatistics(emptyDF)

    result shouldBe None
  }

  // ==================== Tests for getRatingDistribution ====================

  "getRatingDistribution" should "correctly categorize ratings" in {
    val testData = Seq(
      ("Poor Movie", 2.0),
      ("Average Movie", 5.0),
      ("Good Movie", 7.0),
      ("Excellent Movie", 9.0)
    )
    val testDF = createTestDataFrame(testData)
    val distribution = MovieRatingAnalysis.getRatingDistribution(testDF)

    distribution.count() shouldBe 4
  }

  // ==================== Tests for ratingBucket pure function ====================

  "ratingBucket" should "categorize ratings correctly" in {
    MovieRatingAnalysis.ratingBucket(2.0) shouldBe "Poor (< 4)"
    MovieRatingAnalysis.ratingBucket(5.0) shouldBe "Average (4-6)"
    MovieRatingAnalysis.ratingBucket(7.0) shouldBe "Good (6-8)"
    MovieRatingAnalysis.ratingBucket(9.0) shouldBe "Excellent (8+)"
  }

  // ==================== Tests for analysisPipeline ====================

  "analysisPipeline" should "compose filter and statistics correctly" in {
    val testData = Seq(
      ("Movie A", 8.0),
      ("Movie B", 0.0),  // Should be filtered
      ("Movie C", 7.0),
      ("Movie D", -1.0), // Should be filtered
      ("Movie E", 6.0)
    )
    val testDF = createTestDataFrame(testData)
    val result = MovieRatingAnalysis.analysisPipeline(testDF)

    result shouldBe defined
    result.foreach { stats =>
      stats.totalMovies shouldBe 3  // Only valid ratings
      stats.meanRating shouldBe 7.0 +- 0.001
    }
  }

  // ==================== Integration Tests with Real Data ====================

  "Full analysis pipeline" should "process real movie data correctly" in {
    val result = MovieRatingAnalysis.readCsvFile(spark, "data/movie_metadata.csv")
      .map(MovieRatingAnalysis.analysisPipeline)

    result.isSuccess shouldBe true
    result.foreach { statsOpt =>
      statsOpt shouldBe defined
      statsOpt.foreach { stats =>
        stats.totalMovies shouldBe 5043
        stats.meanRating shouldBe 6.44 +- 0.1
        stats.stdDevPopulation shouldBe 1.13 +- 0.1
        stats.minRating shouldBe 1.6 +- 0.1
        stats.maxRating shouldBe 9.5 +- 0.1
      }
    }
  }

  it should "produce consistent results across multiple runs (referential transparency)" in {
    val moviesDF = MovieRatingAnalysis.readCsvFile(spark, "data/movie_metadata.csv").get

    val stats1 = MovieRatingAnalysis.getStatistics(moviesDF)
    val stats2 = MovieRatingAnalysis.getStatistics(moviesDF)

    stats1 shouldBe stats2  // Same input should always produce same output
  }
}