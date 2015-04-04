package com.hanwadikar.paxos

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

object PaxosDemo extends App {

  val system = ActorSystem("mySystem")

  /* We use a majority quorum, so the quorum size is 2 out of 3 */
  val myActor1 = system.actorOf(Props(new Priest(2)), name = "myactor1")
  val myActor2 = system.actorOf(Props(new Priest(2)), name = "myactor2")
  val myActor3 = system.actorOf(Props(new Priest(2)), name = "myactor3")

  implicit val timeout = Timeout(10 seconds)

  val firstRound = myActor1 ? GetSetValue("test")
  val firstRoundRes = Await.result(firstRound, 5 seconds)

  println("\n1st round value: " + firstRoundRes)

  val secondRound = myActor1 ? GetSetValue("testing")
  val secondRoundRes = Await.result(secondRound, 5 seconds)

  println("\n2nd round value: " + secondRoundRes)

  if (firstRoundRes != secondRoundRes) {
    println("Two consecutive paxos rounds should return the same result")
  } else {
    println("Two consecutive paxos rounds returned the same result")
  }

  system.shutdown
}
