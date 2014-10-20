package com.hanwadikar.paxos

import akka.actor.Props
import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.Await
import akka.event.Logging

object PaxosDemo extends App {

  val system = ActorSystem("mySystem")

  val myActor1 = system.actorOf(Props(classOf[Priest], 2), name = "myactor1")
  val myActor2 = system.actorOf(Props(classOf[Priest], 2), name = "myactor2")
  val myActor3 = system.actorOf(Props(classOf[Priest], 2), name = "myactor3")

  val future = myActor1.ask(GetSetValue("test"))(5 seconds)

  println("\nGot future value: " + Await.result(future, 5 seconds))
  system.shutdown
}