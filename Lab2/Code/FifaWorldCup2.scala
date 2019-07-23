import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
//Import the libraries

object FifaWorldCup2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    //Spark Session and Spark Context are Set up
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark RDD queries")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // We are using all 3 Fifa dataset taken from Kaggle Repository
    //Importing  the dataset, creating df, showing the Schema

    val wc_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Rajeshwari\\IdeaProjects\\LAB2\\WorldCups.csv")

    val wcplayers_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Rajeshwari\\IdeaProjects\\LAB2\\WorldCupPlayers.csv")


    val wcmatches_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Rajeshwari\\IdeaProjects\\LAB2\\WorldCupMatches.csv")


    // Schema Printing

    wc_df.printSchema()

    wcmatches_df.printSchema()

    wcplayers_df.printSchema()

    //Temp View creation for all the three Data sets

    wc_df.createOrReplaceTempView("WorldCup")

    wcmatches_df.createOrReplaceTempView("wcMatches")

    wcplayers_df.createOrReplaceTempView("wcPlayers")

    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.

    //We first create the rdd,We already have Dataframe wc_df created

    // RDD creation

    val csv = sc.textFile("C:\\Users\\Rajeshwari\\IdeaProjects\\LAB2\\WorldCups.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()


    //Show all the countries who win along with hosting and year

    //RDD
    val rddvenue = data.filter(line => line.split(",")(1)==line.split(",")(2))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(2)))
      .collect()

    rddvenue.foreach(println)

    //Dataframe
    wc_df.select("Year","Country","Winner").filter("Country==Winner").show(10)


    // Data for Highest Number of Goals

    //RDD
    val rddgoals = data.filter(line => line.split(",")(6) != "NULL").map(line => (line.split(",")(1),
      (line.split(",")(6)))).takeOrdered(10)
    rddgoals.foreach(println)

    // Dataframe
    wc_df.select("Country","GoalsScored").orderBy("GoalsScored").show(10)


    //Details of world cup match in a specific year
    //RDD

    val rddStat = data.filter(line=>line.split(",")(0)=="2006")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddStat.foreach(println)

    //Dataframe
    wc_df.filter("Year=2006").show()

    // Years that end zero
    // RDD
    var years = Array("1930","1950","1970","1990","2010")

    val rddwinY = data.filter(line => (line.split(",")(0)=="1950" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddwinY.foreach(println)

    //DataFrame
    wc_df.select("Year","Runners-Up","Winner").filter("Year='1950' or Year='1930' or " +
      "Year='1990' or Year='1970' or Year='2010'").show(10)


    // Highest number of Matches Played

    //RDD

    val rddMax = data.filter(line=>line.split(",")(8) == "64")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddMax.foreach(println)

    // DataFrame
    wc_df.filter("MatchesPlayed == 64").show()


  }
}
