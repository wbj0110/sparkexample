package example.spark.soledede.rpc

import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory


trait TransferMsg extends Serializable

case class Ping(content: String) extends TransferMsg

case class Pong(content: String) extends TransferMsg

/**
  * Created by soledede on 16/4/12.
  */
class SparkRPCServer extends Actor {

  override def receive: Receive = {
    case ping: Ping => {
      println(s"sparkRpcServe received:${ping.content}")
      sender ! Pong("pong")
    }
    case _ => println("Soledede can't regognise the message!")
  }
}

object SparkRPCServer {
  def main(args: Array[String]) {
    val sparkRpcServerSystem = ActorSystem("soledede", ConfigFactory.parseString(
      """
         akka {
            actor{
                provider = "akka.remote.RemoteActorRefProvider"
            }
            remote {
              enabled-transports = ["akka.remote.netty.tcp"]
              netty.tcp {
                hostname = "127.0.0.1"
                port = "9999"
              }
            }
         }
      """))
    sparkRpcServerSystem.actorOf(Props[SparkRPCServer],"rpcServer")
  }
}

