package example.spark.soledede

import java.io.{File, FileWriter}
import java.util.Random

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by soledede on 16/3/21.
  */
object AvgSalary {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:Average Salary datafile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Example:Average Salary Statistics")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(args(0), 5)
    val count = dataFile.count()
    val salaryData = dataFile.map(line => line.split(" ")(1))
    val totalSalary = salaryData.map(salary => Integer.parseInt(
      String.valueOf(salary))).collect().reduce((x, y) => x + y)
    println("Total Salary:" + totalSalary + ";Number of People:" + count)

    val avgSalary: Double = totalSalary.toDouble / count.toDouble

    println("Average Salary is " + avgSalary)
  }
}


object avgSalaryFileGenerator {

  def main(args: Array[String]) {
    val writer = new FileWriter(new File("/home/salary_data.txt"), false)
    val rand = new Random()
    for (i <- 1 to 10000000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
