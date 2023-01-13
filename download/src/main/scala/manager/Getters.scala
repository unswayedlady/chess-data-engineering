package manager

import data._

import scala.util.Try
import spray.json._
import Parser._
import UnfoldIterator._
import cats.implicits._, cats.data._
import Fold._

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

  def getPlayers(m: Match, s: Set[String]): Option[((Match, List[Profile]), Set[String])] = {
    itCounter.next()
    val ev: StateT[Option, Set[String], List[Option[Profile]]] =
      List(m.white.username, m.black.username).traverse[StateT[Option, Set[String], *], Option[Profile]](
        player => StateT(s => if (!s.contains(player))
          getPlayer(player) match{
            case None => None
            case p => Some((s + player, p))
          }
        else Some((s, None))
        )
      ).map(_.filter(_.isDefined))
    val l_output = ev.runA(s).get.sequence.get
    val s_output = ev.runS(s).get
    Some((m, l_output), s_output)
  }

}
