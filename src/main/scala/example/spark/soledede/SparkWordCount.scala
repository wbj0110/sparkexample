package example.spark.soledede

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soledede on 16/3/21.
  */
object SparkWordCount {
  def FILE_NAME:String = "word_count_results_";
  def main(args:Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word=>(word, 1)).reduceByKey((a, b) => a + b)
    println("Word Count program running results:")
    wordCounts.collect().foreach(e => {
    val (k,v) = e
    println(k+"="+v)
    })
    wordCounts.saveAsTextFile(FILE_NAME+System.currentTimeMillis())
    println("Word Count program running results are successfully saved.")


  }
}

/**
  *standalone
  *
./spark-submit \
--class example.spark.soledede.SparkWordCount \
--master spark://spark1:7077 \
--num-executors 3 \
--driver-memory 6g --executor-memory 2g \
--executor-cores 2 \
/home/sparkexample.jar \
hdfs://spark1:9000/user/hadoop/*.txt
  *
  *
  */