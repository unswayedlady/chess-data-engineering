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

  def getPlayers(m: Match, s: scala.collection.mutable.Set[String]): Option[((Match, Option[List[Profile]]), scala.collection.mutable.Set[String])] = {
    itCounter.next()
    val l : Option[List[Profile]] = List(m.white.username, m.black.username).foldLeft[Option[List[Profile]]](Some(List())){
      case (Some(l), name) if !s.contains(name) => getPlayer(name) match{
        case Some(p) =>
          s += name
          Some(p :: l)
        case None => None
      }
      case (Some(l), _) => Some(l)
      case (None, _) => None
    }
    Some((m, l), s)
  }

}
