package com.lab

import org.apache.spark._
import org.apache.log4j.{Level, Logger}


object FacebookFriend{

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("facefriends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    /** Map function take a line like "5 0 4" and then generates pair of friends
        sorted by id (called the 'key') and output along with the entire friends list.
       Example: pairMapper("4 0 1 2 5") would return the following list
          (0, 4) => 0 1 2 5
          (1, 4) => 0 1 2 5
          (2, 4) => 0 1 2 5
           ( 4, 5) => 0 1 2 5
      */

    def friendsMapper(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }

    /** Reduce function groups by the key and intersects the set with the accumulator to find
       common friends.*/

    def friendsReducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }



    val file = sc.textFile("facebook.txt")

    val results = file.flatMap(friendsMapper)
      .reduceByKey(friendsReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")})

    results.coalesce(1).saveAsTextFile("MutualFriends")




  }

}
