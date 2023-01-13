package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFramesBasics.{cars, spark}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data sources and formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))


  /**
    * Reading a DF:
    * - format
    * - schema or inferSchema = true
    * - zero or more options
    */
  private val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") //dropMalformed, permissive (default), `error handling when loading data`
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()

  //Alternative to have multiple `.option` statements
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFest",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /**
    * Writing DFs
    * - format
    * - save mode = overwrite, append, ignore, errorIfExists
    * - path where we need to save the DF to
    * - zero or more .options
    */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  //JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") //couple with schema; if Spark fails parsing, it will put a null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  //CSV flags
  private val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "") //important, no notion of nulls in .csv format
    .csv("src/main/resources/data/stocks.csv")

  //Parquet - open source compressed binary data format: very important
  carsDF.write
    //.format("parquet") //Not needed, since parquet is the default file format of Spark!
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  //Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading from a remote DB
  private val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise:
    * 1) Read the movies DataFrame (movies.json) and write it as three different things
    *  - tab-separated values file
    *  - snappy parquet
    *  - table public.movies in the Postgres DB ??
    */


  //1.
  private val moviesDF = spark.read
    .format("json")
    .json("src/main/resources/data/movies.json")

  //writing the moviesDF as a tab-separated file
  moviesDF.write
    .format("tsv")
    .option("sep", "  ")
    .option("nullValue", "")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/moviestsv")

  //writing as a snappy parquet
  moviesDF.write
    .format("parquet")
    .option("compression", "snappy") //bzip2, gzip, lz4, snappy, deflate
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/moviessnappyparquet")


}
