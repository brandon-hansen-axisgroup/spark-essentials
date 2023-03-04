package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, desc, mean, stddev}

object Aggregations extends App {

  //Boiler plate stuff

  val spark = SparkSession.builder()
    .appName("aggreations and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //Counting
  private val genresCountDF = moviesDF.select(count(col("Major_Genre"))) //all values except null
  moviesDF.selectExpr("count(Major_Genre)") //does the same thing

  //Counting all
  moviesDF.select(count("*")) //count all the rows, and will INCLUDE the null values

  //Counting distinct values
  private val distinctGenres = moviesDF.select(countDistinct(col("Major_Genre")))

  //approximate version
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) //useful for very big data, approx row count

  //min and max
  val minRatingDF = moviesDF.select(functions.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)") //does the same thing as the above

  val maxRatingDF = moviesDF.select(functions.max(col("IMDB_Rating")))
  moviesDF.selectExpr("max(IMDB_Rating)") //does the same thing as the above

  //sum
  moviesDF.select(functions.sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //average
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  //data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  )

  //grouping
  private val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() //select count(*) from moviesDF groupBy 'Major_Genre'

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg(col("IMDB_Rating")).as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
    * Exercises:
    * 1. Sum all the profits of all of the movies in the DF
    * 2. Count how many distinct directors we have in the DF
    * 3. Show the mean and SD of US Gross Revenue for the movies
    * 4. Compute the average IMDB_Rating and the average US_Gross_Revenue per director
    */
  //"Title":"The Land Girls","US_Gross":146083,"Worldwide_Gross":146083,"US_DVD_Sales":null,"Production_Budget":8000000,"Release_Date":"12-Jun-98","MPAA_Rating":"R","Running_Time_min":null,"Distributor":"Gramercy","Source":null,"Major_Genre":null,"Creative_Type":null,"Director":null,"Rotten_Tomatoes_Rating":null,"IMDB_Rating":6.1,"IMDB_Votes":1071}


  //1.
  private val sumAllMoviesDF = moviesDF
    .select(functions.sum(col("US_Gross")))
    .as("Sum of All Profits")
  sumAllMoviesDF.show()

  //2.
  private val distinctDirectorsDF = moviesDF
    .groupBy(col("Director"))
    .count().as("Movies Directed")
  distinctDirectorsDF.show()

  //3.
  private val meanAndSdDF = moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )
  meanAndSdDF.show()

  //4.
  private val grossPerDirectorDF = moviesDF
    .groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("Avg_Rating"),
      avg(col("US_Gross")).as("Avg_Gross")
    )

  grossPerDirectorDF.show()


}
