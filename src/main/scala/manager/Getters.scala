package manager

import data._

import scala.util.Try
import spray.json._
import Parser._
import UnfoldIterator._

// Getters that intend to finally obtain matches and players' data

object Getters {

  def getTitledPlayers(title: String): Iterator[String] = {
    Try(requests
      .get("https://api.chess.com/pub/titled/" + title)
      .text
      .parseJson
      .convertTo[Map[String, List[String]]].values.head)
      .fold(t => {println(t.getMessage); List()}, l=>l)
      .iterator
  }

  def getPlayerTournaments(playerId: String): Iterator[String] = {
    Try(requests
      .get("https://api.chess.com/pub/player/" + playerId + "/tournaments")
      .text
      .parseJson
      .convertTo[Map[String, List[PlayerTournament]]].values.head
      .map(_.id))
      .fold(t => {println(t.getMessage); List()}, l=>l)
      .iterator
  }

  def getRounds(tournamentId: String) : Iterator[String] = {
    Try(
      requests
        .get(tournamentId)
        .text()
        .parseJson
        .convertTo[Tournament]
        .rounds)
      .fold(t => {println(t.getMessage); List()}, l=>l)
      .iterator
  }

  def getGroups(roundId: String) : Iterator[String] = {
    Try(requests
      .get(roundId)
      .text()
      .parseJson
      .convertTo[Round]
      .groups)
      .fold(t => {println(t.getMessage); List()}, l=>l)
      .iterator
  }

  def getMatch(groupId: String) : Iterator[Match] = {

    Try(requests
      .get(groupId)
      .text()
      .parseJson
      .convertTo[MatchArchive]
      .games)
      .fold(t => {println(t.getMessage); List()}, l=>l)
      .iterator
  }

  def getPlayer(user: String): Option[Profile] = {
    Try(requests
      .get("https://api.chess.com/pub/player/" + user)
      .text
      .parseJson
      .convertTo[Profile])
      .fold(t => {println(t.getMessage); None}, Some(_))
  }

  val itCounter: Iterator[Unit] = Iterator.unfold[Unit, Int](1) { n =>
    if (n % 100 == 0) {
      println("We had processed so far " + n + " requests...")
    }
    Some(((), n + 1))
  }

  implicit class TraverseOp[A](l: List[A]){
    def traverse[B](f: A => Option[B])(s: Set[A]): (Option[List[B]], Set[A]) = {
      l.foldLeft[(Option[List[B]], Set[A])](Some(List()), s){
        case ((Some(l), s), a) if !s.contains(a) => f(a) match {
          case Some(h) => (Some(h :: l), s+a)
          case None => (None, s)
        }
        case ((Some(l), s), _) => (Some(l), s)
        case ((None, s), _) => (None, s)
      }
    }
  }

  def getPlayers(m: Match, s: Set[String]): Option[((Match, Option[List[Profile]]), Set[String])] = {
    itCounter.next()
    val (l, s2) : (Option[List[Profile]], Set[String]) = List(m.white.username, m.black.username)
      .traverse(getPlayer)(s)
    Some((m, l), s2)
  }

}
