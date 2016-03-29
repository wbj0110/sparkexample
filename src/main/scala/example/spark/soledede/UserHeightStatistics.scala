package example.spark.soledede

import java.io.{File, FileWriter}

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
  * Created by soledede on 16/3/22.
  */

object DataFileGeneral {
  def main(args: Array[String]) {

    val writer = new FileWriter(new File("/home/user_height_datafile.txt"), false)
    val rand = new Random()
    for (i <- 1 to 9000000) {
      var height = rand.nextInt(220)
      if (height < 50) {
        height = height + 50
      }
      val tribe = getRandomTribe
      if (height < 100 && tribe == "サスケ")
        height = height + 100
      if (height < 100 && tribe == "ナルト")
        height = height + 50
      writer.write(i + " " + getRandomTribe + " " + height)
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    println("User Height generated successfully.")
  }

  def getRandomTribe(): String = {
    val rand = new Random()
    val randNum = rand.nextInt(2) + 1
    if (randNum % 2 == 0) {
      "サスケ"
    } else {
      "ナルト"
    }
  }
}


object UserHeightStatistics {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:UserHeight datafile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Example:User Height (Tribe & Height) Statistics")
    val sc = new SparkContext(conf)

    val dataFile = sc.textFile(args(0), 5)

    val sData = dataFile.filter(line => line.contains("ぅちは")).map(
      line => (line.split(" ")(1) + " " + line.split(" ")(2)))

    val nData = dataFile.filter(line => line.contains("木ノ葉")).map(
      line => (line.split(" ")(1) + " " + line.split(" ")(2)))


    val sHeightData = sData.map(line => line.split(" ")(1).toInt)
    val nHeightData = nData.map(line => line.split(" ")(1).toInt)

    val lowestS = sHeightData.sortBy(x => x, true).first()
    val lowestN = nHeightData.sortBy(x => x, true).first()

    val highestS = sHeightData.sortBy(x => x, false).first()
    val highestN = nHeightData.sortBy(x => x, false).first()


    println(s"Number of ぅちはの集落 Peole:${sData.count()}")
    println(s"Number of 木ノ葉の集落 Peole:${nData.count()}")
    println(s"Lowest  ぅちは:$lowestS")
    println(s"Lowest  木ノ葉:$lowestN")
    println("------------------------------")
    println(s"Highest ぅちは:$highestS")
    println(s"Highest 木ノ葉:$highestN")
  }
}
