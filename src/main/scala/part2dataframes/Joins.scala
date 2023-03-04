package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true").
    json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  //joins
  private val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "inner"
  )

  //outer joins
  //left outer = everything in inner join + all rows in the left table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  //right outer = everything in the inner join + all the rows in the right table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  //outer join = everything in the inner join + all rows in both tables, with nulls where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  //semi-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")


}
