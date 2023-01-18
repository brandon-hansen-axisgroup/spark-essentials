package part2dataframes

import org.apache.commons.lang3.BooleanUtils.and
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder().appName("DF Columns and Expressions").config("spark.master", "local").getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  carsDF.show()

  //Columns
  private val firstColumn = carsDF.col("Name")
  //selecting (projecting)
  private val carNamesDF = carsDF.select(firstColumn)

  //various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("name"),
    carsDF.col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", //fancier interpolated string, returns a Column object
    expr("Origin") //EXPRESSION
  )

  //select with plain column names
  carsDF.select("Name", "Year")

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  private val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2

  private val carsWithWeightsDF = carsDF.select(col("Name"), col("Weight_in_lbs"), weightInKGExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2"))


  //selectEpr
  val carsWithSelecExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //DF processing

  //adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  //renaming a column
  private val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  //careful with column names
  carsWithColumnRenamed.selectExpr("'Weight in pounds'")

  //remove column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //filtering
  val EuroCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val euroCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  //filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") //Like an SQL statement inside as string

  //unioning = adding more rows
  private val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) //works only if two DataFrames have the same schema

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()

  /**
    * Exercise(s)
    * 1. read movies.json and select two columns of your choice
    * 2. Create a new column by adding columns US_Gross, Worldwide_Gross, US_DVD_Sales as total_profit
    * 3. Select all comedy movies (Major_Genre) with IMDB rating > 6
    *
    * Use as many different ways as possible for each exercise, although its not necessary
    */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //1.
  private val movieAndDirector = moviesDF.select(
    col("Creative_Type"),
    col("Director")
  )
  movieAndDirector.show()

  //2.
  private val grossIncomeMovies = moviesDF.select("Title", "Worldwide_Gross", "US_Gross").withColumn("total_profit", col("US_Gross") + col("Worldwide_Gross"))
  grossIncomeMovies.show()

  //3.
  private val goodComedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()
  private val goodComedyMovie2 = moviesDF.filter(col("Major_Genre") === "Comedy").filter(col("IMDB_Rating") > 6)
  private val goodComedyMovie3 = moviesDF.filter("Major_Genre = 'Comedy' AND IMDB_Rating > 6")


}
