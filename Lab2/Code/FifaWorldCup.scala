package com.lab

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import the libraries

object FifaWorldCup {

  def main(args: Array[String]): Unit = {

    //Spark Session and Spark Context Setup Point
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("FIFA")
      .config(conf = conf)
      .getOrCreate()


    // Using StructType and creating Worldcup Dataframes
    val customSchema = StructType(Array(
      StructField("Year", IntegerType, true),
      StructField("Country", StringType, true),
      StructField("WinnerTeam", StringType, true),
      StructField("Runners-UpTeam", StringType, true),
      StructField("ThirdTeam", StringType, true),
      StructField("FourthTeam", StringType, true),
      StructField("GoalsScoredTotal", IntegerType, true),
      StructField("QualifiedTeamsTotal", IntegerType, true),
      StructField("MatchesPlayedTotal", IntegerType, true),
      StructField("AttendanceTotal", DoubleType, true)))

    val worldcup_df = spark.sqlContext.read.format("csv")
      .option("delimiter",",")
      .option("header", "true")
      .schema(customSchema)
      .load("WorldCups.csv")

    //Teams who won highest WorldCups

    worldcup_df.groupBy("WinnerTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Countries that hosted highest WorldCups

    worldcup_df.groupBy("Country").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Countries that won the WorldCup and Hosted them

    worldcup_df.createGlobalTempView("worldcup")

    spark.sql("SELECT Country,WinnerTeam FROM global_temp.worldcup where Country==WinnerTeam").show()

    // From years 1930 to 2014 total number of Goals Scored

    worldcup_df.agg(sum("GoalsScoredTotal")).show()

    // Top teams in Runner-Up

    worldcup_df.groupBy("Runners-UpTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Percentage of Goals required to Qualify for the match

    import spark.sqlContext.implicits._

    worldcup_df.withColumn("Percentage",$"QualifiedTeamsTotal"/$"GoalsScoredTotal"*100).show()

    //Matches with highest attendance

    worldcup_df.agg(max("AttendanceTotal")).show

    //Top teams that placed in ThirdTeam

    worldcup_df.groupBy("ThirdTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Top teams that placed in FourthTeam

    worldcup_df.groupBy("FourthTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Total number of Goals scored by each country in all the years

    spark.sql("SELECT Country,sum(GoalsScoredTotal) FROM global_temp.worldcup group by Country").show()


  }

}
