package example.spark.soledede.clustering

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by soledede on 16/3/29.
  */
object KMeansClustering {

  def main(args: Array[String]) {
    if (args.length < 5) {

      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters  numIterations runTimes")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark MLlib Example:K-Means Clustering")

    val sc = new SparkContext(conf)

    /**
      * Channel Region Fresh  Milk  Grocery Frozen  Detergents_Paper  Delicassen
      * 2	    3	    12669	  9656	7561	  214	    2674	            1338
      * 2	    3   	7057 	  9810	9568  	1762  	3293            	1776
      * 2   	3   	6353  	8808	7684  	2405  	3516            	7844
      * 1   	3   	13265	  119  	4221  	6404  	507	              1788
      */

    val tainingData = sc.textFile(args(0))
    val parsedTrainingData = tainingData.filter(!isColumnNameChannelLine(_)).map(line => {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()


    val numClusters = args(2).toInt
    val numIterations = args(3).toInt
    val runTimes = args(4).toInt
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)

    println("Cluster Number:" + clusters.clusterCenters.length)

    println("Cluster Centers  Overview:")
    clusters.clusterCenters.foreach(x => {
      println(s"Center Point of Cluster $clusterIndex:$x")
      clusterIndex += 1
    })

    //check which cluster each test data belongs to based on the clustering result

    val testData = sc.textFile(args(1))
    val parsedTestData = testData.map(line => {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(line => {
      val predictedClusterIndex: Int = clusters.predict(line)
      println(s"The data $line.toString  belongs to cluster $predictedClusterIndex")
    })
    println("Spark MLlib K-means clustering test finished.")
  }

  //filter the first channel
  private def isColumnNameChannelLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}
