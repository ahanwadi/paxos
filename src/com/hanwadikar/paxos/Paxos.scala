package com.hanwadikar.paxos

import akka.actor.Actor
import akka.event.Logging
import scala.collection.mutable
import akka.actor.ActorRef
import akka.actor.Props
import scala.concurrent.duration._
import akka.actor.ReceiveTimeout

/*
 * Three requirements:
 * B1 - Each ballot has a unique number
 * B2 - The quorum of any two ballots has at least one priest in common.
 * B3 - If any priest from the ballot's quorum has voted earlier, then decree of this ballot
 *      is same as the decree of the highest earlier ballot in which any of the priest has voted. 
 */

case class BallotNum(priest: PriestId, ballot: Int) extends Ordered[BallotNum] {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: BallotNum) = (this.ballot, this.priest) compare (that.ballot, that.priest)
}

case class Decree(num: Int, value: String)
abstract class Vote(val priest: PriestId)
case class ValidVote(num: BallotNum, override val priest: PriestId, decree: Decree) extends Vote(priest)
case class NullVote(override val priest: PriestId) extends Vote(priest)

case class NextBallotMsg(num: BallotNum)
case class LastVoteMsg(num: BallotNum, vote: Vote)
case class BeginBallotMsg(num: BallotNum, decree: Decree)
case class VotedMsg(num: BallotNum, priest: PriestId)
case class SuccessMsg(decree: Decree)
case class GetSetValue(value: String)

case class GetResultMsg()
case class ResultMsg(num: BallotNum, quorumSize: Int, d: Decree, done: Boolean)

/*
 * Represents a ballot in progress.
 * Each ballot has its own quorum, decree that was selected and its unique number.
 */
class Ballot(b: BallotNum, quorumSize: Int, var d: Decree, listener: ActorRef) extends Actor {
  val log = Logging(context.system, this)
  val votes: mutable.MutableList[ValidVote] = mutable.MutableList()
  var quorum: Set[PriestId] = Set[PriestId]()

  private def handleVote(q: PriestId, v: Vote) = {
    if (quorum.size == quorumSize) {
      log.info("Ballot closed")
    } else if (!quorum.contains(q)) {
      quorum = quorum + q
      v match {
        case x: ValidVote =>
          votes += x
        case _ =>
      }

      if (quorum.size == quorumSize) {
        log.info("Ballet ready to begin")
        d = votes.sortBy { v => v.num }.lastOption.map(_.decree).getOrElse(d)
        quorum.map(q => context.actorSelection("../../" + q)).foreach(a => a ! BeginBallotMsg(b, d))
      }
    }
  }

  /*
   *  We start a new ballot by sending NextBal message to all
   * participants.
   */
  context.actorSelection("../../*") ! NextBallotMsg(b)

  def receive: Receive = {
    case lvm @ LastVoteMsg(b, v @ NullVote(q)) =>
      log.info("Got last vote message: {}", lvm)
      handleVote(q, v)

    case lvm @ LastVoteMsg(b, v @ ValidVote(_, q, _)) =>
      log.info("Got last vote message: {}", lvm)
      handleVote(q, v)

    case VotedMsg(b, q) =>
      log.info("Received vote for ballot = {} from priest {}", b, q)
      if (quorum.size > 0) {
        quorum -= q
        if (quorum.size == 0) {
          log.info("Ballot successful: received votes from all quorum members")
          listener ! SuccessMsg(d)
          context.actorSelection("../../*") ! SuccessMsg(d)
        }
      }

    case GetResultMsg =>
      sender ! ResultMsg(b, quorumSize, d, quorum.size == 0)
  }
}

/**
 * Priest conducts and participates in the ballot.
 */
class Priest(quorumSize: Int) extends Actor {
  val log = Logging(context.system, this)

  var myId: PriestId = self.path.name
  var nextBalNum: Int = -1

  var nextBal: Option[BallotNum] = None

  // All my previous votes
  var myVotes: mutable.MutableList[Vote] = mutable.MutableList[Vote]()

  myVotes += NullVote(priest = myId)

  var decrees: mutable.MutableList[Decree] = mutable.MutableList[Decree]()

  def receive = conductor orElse elector

  def conductor: Receive = {
    case GetSetValue(value) =>
      log.info("Got request to get or set value: {}", value)
      /*
       * Even to read a value we have to conduct an election.
       */
      nextBalNum += 1
      val b = BallotNum(myId, nextBalNum)
      context.actorOf(Props(classOf[Ballot], b, 2, Decree(0, value), sender()))
  }

  def elector: Receive = {
    case "test" => log.info("received test")

    case NextBallotMsg(b) =>
      log.info("Received next ballot - {}", b)

      /*
       * To satisfy B2, we have to ensure that any ballot's decree is chosen
       * from the highest ballot in which any of the quorum priest has voted.
       */

      if (nextBal.isEmpty || b > nextBal.get) {
        nextBal = Some(b)
        log.info("Sending last vote message: {}", myVotes.last)
        sender ! LastVoteMsg(b, myVotes.last)
      }

    case bb @ BeginBallotMsg(b, d) if nextBal == Some(b) =>
      log.info("Received Begin Ballot = {}", bb)
      myVotes += ValidVote(b, myId, d)
      sender ! VotedMsg(b, myId)

    case SuccessMsg(d) =>
      log.info("Received ballot success message: {}", d)
      decrees += d

  }
}