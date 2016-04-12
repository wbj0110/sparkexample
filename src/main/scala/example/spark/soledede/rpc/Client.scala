package example.spark.soledede.rpc

import akka.actor._
import com.typesafe.config.ConfigFactory
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

import scala.concurrent.Await

/**
  * Created by soledede on 16/4/12.
  */
class Client extends Actor {
  var remoteActor: ActorSelection = _
  var localActor: ActorRef = _

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    remoteActor = context.actorSelection("akka.tcp://soledede@127.0.0.1:9999/user/rpcServer")
    println(s"remote address:$remoteActor")
  }

  override def receive: Receive = {
    case ping: Ping => {
      println(s"client:${ping.content}")
      this.localActor = sender()
      remoteActor ! ping
    }
    case pong: Pong => {
      localActor ! pong
    }
    case _ => println("client can't recognise this message!")
  }
}

object Client {
  def main(args: Array[String]) {
    val clientSystem = ActorSystem("client", ConfigFactory.parseString(
      """
        akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
        }
      """))

    var client = clientSystem.actorOf(Props[Client])

    val responseFuture = client ? Ping("ping")

    implicitly val timeout = Timeout(5 seconds)

    val result = Await.result(responseFuture, timeout.duration).asInstanceOf[Pong]

    println(s"client recieved:${result.content}")

    clientSystem.shutdown()

  }

}