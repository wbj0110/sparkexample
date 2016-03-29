package example.spark.soledede.sparkstreaming

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.util.Random

/**
  * Created by soledede on 16/3/22.
  */

class UserBehaviorProducter(brokers: String, topic: String) extends Runnable {

  private val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  private val PRODUCT_NUM = 100
  private val MAX_MSG_NUM = 3
  private val MAX_CLICK_TIME = 5
  private val MAX_STAY_TIME = 10

  //Added cart,1;ditn't add cart 0
  private val ADD_CART = Array[Int](1, 0)

  def run(): Unit = {
    val rand = new Random()
    while (true) {
      val msgNum = rand.nextInt(MAX_MSG_NUM) + 1
      try {

        //soledede/product.html?id=23413|1|1.2|1
        for (i <- 0 to msgNum) {
          var msg = new StringBuilder()
          msg.append("soledede/product.html?id=" + (rand.nextInt(PRODUCT_NUM) + 1))
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat())
          msg.append("|")
          msg.append(ADD_CART(rand.nextInt(2)))
          println(msg.toString())
          sendMessage(msg.toString())
        }
        println("%d user behavior messages produced.".format(msgNum + 1))
      } catch {
        case e: Exception => println(e)
      }
      try {
        //sleep for 5 seconds after send a batch of message
        Thread.sleep(5000)
      } catch {
        case e: Exception => println(e)
      }
    }
  }

  def sendMessage(message: String) = {
    try {
      val data = new KeyedMessage[String, String](this.topic, message)
      producer.send(data)
    } catch {
      case e: Exception => println(e)
    }
  }
}

object UserBehaviorProducerClient {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.exit(1)
    }
    new Thread(new UserBehaviorProducter(args(0), args(1))).start()
  }
}


object PopularityStatistics {
  private val checkpointDir = "popularity-data-checkpoint"
  private val group = "goup1"
  private val consumeTopic = "userBehavior"

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:PopularityStatistics zk1:2181,zk2:2181,zk3:2181 consumeMsgDataTimeInterval (secs) ")
      System.exit(1)
    }
    val Array(zk, processingInterval) = args
    val conf = new SparkConf().setAppName("Product PopularityStatistics")
    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))



    //using updateStateByKey asks for enabling checkpoint
    ssc.checkpoint(checkpointDir)

    val kafkaStream = KafkaUtils.createStream(
      //Spark streaming context
      ssc,
      //zookeeper quorum. e.g zk1:2181,zk2:2181
      zk,
      //kafka message consumer group ID
      group,
      //Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
      Map(consumeTopic -> 3))


    val msgDataRDD = kafkaStream.map(_._2)
    // soledede/product.html?id=23413|1|1.2|1


    val popularityData = msgDataRDD.map { msgLine => {
      val dataArr: Array[String] = msgLine.split("\\|")
      val productPageID = dataArr(0)

      //calculate the popularity value
      val popValue: Double = dataArr(1).toFloat * 0.7 + dataArr(2).toFloat * 0.5 + dataArr(3).toFloat * 0.8
      (productPageID, popValue)
    }
    }


    //sum the previous popularity value and current value
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }


    val initialRDD = ssc.sparkContext.parallelize(List(("soledede/product.html?id=23413", 0.00)))


    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)


    //set the checkpoint interval to avoid too frequently data checkpoint which
    //may significantly reduce operation throughput
    stateDstream.checkpoint(Duration(8 * processingInterval.toInt * 1000))

    //after calculation, we need to sort the result and only show the top 20 hot product
    stateDstream.foreachRDD { rdd => {
      val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
      val topKData = sortedData.take(20).map { case (v, k) => (k, v) }
      topKData.foreach(x => {
        println(x)
      })
    }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
