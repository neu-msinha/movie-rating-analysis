# Movie Rating Analysis - CSYE7200 Assignment

A Spark-based application to analyze movie ratings from the IMDB 5000 Movie Dataset using functional programming principles in Scala.

## Dataset Source
- **Source**: Kaggle IMDB 5000 Movie Dataset
- **URL**: https://www.kaggle.com/datasets/carolzhangdc/imdb-5000-movie-dataset
- **File**: `movie_metadata.csv`

## Requirements
- **Scala**: 2.12.15
- **Spark**: 3.2.1
- **SBT**: 1.8.x or 1.9.x
- **Java**: 11

## Project Structure
```
movie-rating-analysis/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   ├── main/
│   │   ├── scala/edu/neu/csye7200/movierating/
│   │   │   ├── MovieRatingAnalysis.scala
│   │   │   └── MovieRatingApp.scala
│   │   └── resources/
│   │       └── log4j.properties
│   └── test/scala/edu/neu/csye7200/movierating/
│       └── MovieRatingAnalysisSpec.scala
└── data/
    └── movie_metadata.csv
```

## How to Run

### Using SBT
```bash
# Compile
sbt compile

# Run tests
sbt test

# Run application
sbt run
```

### Using IntelliJ IDEA
1. Open the project in IntelliJ
2. Wait for SBT to import dependencies
3. Right-click on `MovieRatingApp` → Run
4. Right-click on `MovieRatingAnalysisSpec` → Run Tests

## Results

| Metric | Value |
|--------|-------|
| Total Movies Analyzed | 5043 |
| Mean IMDB Score | 6.4421 |
| Standard Deviation (Population) | 1.1250 |
| Standard Deviation (Sample) | 1.1251 |
| Minimum Rating | 1.6 |
| Maximum Rating | 9.5 |

## Features
- Reads CSV using Spark's built-in CSV data source
- Calculates mean and standard deviation of IMDB scores
- Filters invalid/null ratings
- Provides rating distribution analysis
- Includes 16 comprehensive test cases