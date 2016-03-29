package example.spark.soledede

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by soledede on 16/3/22.
  */
object SearchKeywordsTopN {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:TopNSearchKeyWords")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Example:Top N Searching Key Words")
    val sc = new SparkContext(conf)
    val keywordsData = sc.textFile(args(0))

    val reduceKeywordsData = keywordsData.map(line => (line.toLowerCase(), 1)).reduceByKey((x, y) => x + y)

    val sortedkeywordData = reduceKeywordsData.map { case (k, v) => (v, k) }.sortByKey(false)

    val topNData = reduceKeywordsData.take(args(1).toInt).map { case (v, k) => (k, v) }
    topNData.foreach(println)
  }

}
