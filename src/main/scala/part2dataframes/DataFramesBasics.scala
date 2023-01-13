package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {


  //Creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  //Reading a DataFrame
  private val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  //showing a DF
  firstDF.show()
  //showing the schema of DF
  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  //spark types
  val longType = LongType
  val doubleType = DoubleType
  val stringType = StringType
  // ...

  //schema -- manually
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //obtain schema of existing data set
  private val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  //read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  //create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  //create DF from tuples
  val cars = Seq(
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  private val manualCarsDF = spark.createDataFrame(cars) //schema auto-inferred
  print(manualCarsDF)

  //note: DFs have schema, rows do not

  //create DFs with implicits

  import spark.implicits._

  private val manualCarsDFWithImplicits = cars.toDF("Name",
    "MPG",
    "Cylinders",
    "Displacement",
    "HP",
    "Weight",
    "Acceleration",
    "Year",
    "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /**
    * Exercise(s):
    * 1) Create a manual DF describing smart phones
    *  - make, model, processor, screen dimension, camera megapixels
    * 2) Read another file from the data/ folder (movies dataset)
    *  - print its schema
    *  - count the number of rows by calling the .count method
    */

  //1.
  private val smartPhones = Seq(
    ("iPhone", "14", "M2", "14x6", 220),
    ("Samsung Galaxy", "S8", "i7", "14x10", 280),
    ("Google Pixel 3", "Pixel", "Google Chip", "14x6", 320),
  )
  private val smartPhoneDF = smartPhones.toDF("make", "model", "processor", "screen dimension", "camera megapixels")
  smartPhoneDF.show()

  //2.
  private val moviesData = spark.read
    .json("src/main/resources/data/movies.json")
  moviesData.printSchema()
  println(moviesData.count)


}
