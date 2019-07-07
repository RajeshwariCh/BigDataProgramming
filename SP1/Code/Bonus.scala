package com.spark.demo

object Bonus {
  def main(args: Array[String]) {
    val fruit: String = "RajeshwariReddyCholleti".toLowerCase
    val map = scala.collection.mutable.HashMap.empty[Char, Int]
    for (symbol <- fruit) {
      if (map.contains(symbol))
        map(symbol) = map(symbol) + 1
      else
        map.+=((symbol, 1))
    }
    println(map)

  }
}
