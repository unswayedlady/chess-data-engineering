
import org.apache.spark.sql.SparkSession
import spray.json._

import java.io.FileWriter
import java.sql.Timestamp
import scala.language.postfixOps
import scala.util.Try

object Descarga {

  case class PlayerList(players: List[String])

  case class Profile(player_id: Long, // the non-changing Chess.com ID of this player
                     username: String, // the username of this player
                     title: Option[String], // (optional) abbreviation of chess title, if any
                     status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
                     country: String, // API location of this player's country's profile
                     followers: Int, // the number of players tracking this player's activity
                     is_streamer: Boolean, //if the member is a Chess.com streamer
                     joined: Timestamp // timestamp of registration on Chess.com
                    ) {
    override def equals(obj: Any): Boolean = {
      obj match{
        case Profile => this.player_id.equals(obj.asInstanceOf[Profile].player_id)
        case _ => false
      }

    }
  }

  case class PlayerTournament(id: String) // id of player's tournament
  case class PlayerTournamentList(finished: List[PlayerTournament]) // list of tournaments
  // played by user

  case class Tournament(rounds: List[String]) // link of rounds of tournaments

  case class Round(groups: List[String]) // link of groups of rounds of tournaments

  case class Player(username: String, result: String) // player username + result

  case class Match(white: Player, //white player
                   black: Player, //black player
                   eco: Option[String], // opening
                   pgn: String, // current PGN
                     )

  case class MatchList(games: List[Match])

  object myJsonProtocol extends DefaultJsonProtocol{

    implicit object TimestampJsonFormat extends JsonFormat[Timestamp] {
      def write(x: Timestamp): JsNumber = JsNumber(x.getTime)
      def read(value: JsValue): Timestamp = value match {
        case JsNumber(x) => new Timestamp(x.longValue)
        case x => deserializationError("Expected Timestamp as JsNumber, but got " + x)
      }
    }

    implicit val jsonFormatterTitledPlayers: RootJsonFormat[PlayerList] = jsonFormat1(PlayerList.apply)
    implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat8(Profile.apply)
    implicit val jsonFormatterPlayerTournamentNode: RootJsonFormat[PlayerTournament] = jsonFormat(PlayerTournament, "@id")
    implicit val jsonFormatterPlayerTournament: RootJsonFormat[PlayerTournamentList] = jsonFormat1(PlayerTournamentList.apply)
    implicit val jsonFormatterTournament: RootJsonFormat[Tournament] = jsonFormat1(Tournament.apply)
    implicit val jsonFormatterRounds: RootJsonFormat[Round] = jsonFormat1(Round.apply)
    implicit val jsonFormatterPlayer: RootJsonFormat[Player] = jsonFormat2(Player.apply)
    implicit val jsonFormatterGameNode: RootJsonFormat[Match] = jsonFormat4(Match.apply)
    implicit val jsonFormatterGame: RootJsonFormat[MatchList] = jsonFormat1(MatchList.apply)
  }

  /** Creates an iterator that uses a function `f` to produce elements of
   * type `A` and update an internal state of type `S`.
   */
  final class UnfoldIterator[A, S](init: S)(f: S => Option[(A, S)]) extends Iterator[A] {
    private[this] var state: S = init
    private[this] var nextResult: Option[(A, S)] = null

    override def hasNext: Boolean = {
      if (nextResult eq null) {
        nextResult = {
          val res = f(state)
          if (res eq null) throw new NullPointerException("null during unfold")
          res
        }
        state = null.asInstanceOf[S] // allow GC
      }
      nextResult.isDefined
    }

    override def next(): A = {
      if (hasNext) {
        val (value, newState) = nextResult.get
        state = newState
        nextResult = null
        value
      } else Iterator.empty.next()
    }
  }

  implicit class UnfoldOf[A](it: Iterator.type) {
    def unfold[A, S](init: S)(f: S => Option[(A, S)]): Iterator[A] =
      new UnfoldIterator(init)(f)
  }

  def main(args: Array[String]): Unit ={

    import myJsonProtocol._

    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.dir", "C:\\Users\\milam\\OneDrive\\Escritorio\\spark-events")
      .config("spark.history.fs.logDirectory", "C:\\Users\\milam\\OneDrive\\Escritorio\\spark-events")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1.- Get players id method

    def getPlayers(title: String): Iterator[String] = {
      Try(requests
        .get("https://api.chess.com/pub/titled/" + title)
        .text
        .parseJson
        .convertTo[PlayerList]
        .players)
        .fold(t => {println(t.getMessage); List()}, l=>l)
        .iterator
    }

    // 2.- Get players tournaments id method

    def getPlayerTournaments(playerId: String): Iterator[String] = {
      Try(requests
        .get("https://api.chess.com/pub/player/" + playerId + "/tournaments")
        .text
        .parseJson
        .convertTo[PlayerTournamentList]
        .finished
        .map(_.id))
        .fold(t => {println(t.getMessage); List()}, l=>l)
        .iterator
    }

    // 3.- Get rounds id method

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

    // 4.- Get groups id method

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

    // 5.- Get match method

    def getMatch(groupId: String) : Iterator[Match] = {
      Try(requests
        .get(groupId)
        .text()
        .parseJson
        .convertTo[MatchList]
        .games)
        .fold(t => {println(t.getMessage); List()}, l=>l)
        .iterator

    }

    // 6.- Get player method

    def getPlayer(user: String): Option[Profile] = {
      Try(requests
        .get("https://api.chess.com/pub/player/" + user)
        .text
        .parseJson
        .convertTo[Profile])
        .fold(t => {println(t.getMessage); None}, Some(_))
    }

    // Iterator to count processed requests

    val itCounter : Iterator[Unit] = Iterator.unfold[Unit, Int](0){ n =>
      if (n % 100 == 0) {
        println("We had processed so far " + n + " requests...")
      }
      Some(((), n + 1))
    }

    // Implicit class => unfold

    implicit class Op[A, S, M](it: Iterator[M]){
      def unfold2(s : S)(f: ((S, Iterator[M])) => Option[(A, (S, Iterator[M]))]): Iterator[A] = {
        Iterator.unfold((s, it))(f)
      }
    }

    val matchesFile = new FileWriter("matches.json")
    val playersFile = new FileWriter("players.json")


    // Getting the matches iterator

    List("GM", "WGM", "IM", "WIM", "FM", "WFM", "CM", "WCM")
          .iterator
          .flatMap(t => {
            println("Getting " + t + " titled players...")
            getPlayers(t)
          })
          .flatMap(p => {
            println("Obtaining " + p + " tournaments... ")
            getPlayerTournaments(p)
          })
          .flatMap(r => {
            println("Obtaining from " + r + " round's info... ")
            getRounds(r)
          })
          .flatMap(g => {
            println("Getting from " + g + " group's info... ")
            getGroups(g)
          })
          .flatMap(m => {
            itCounter.next()
            getMatch(m)
          })
          .unfold2(Set[Profile]())({
            case (s, it) =>
              if (!it.hasNext) None
              else {
                var m : Match = it.next
                var whitePlayerOpt : Option[Profile] = getPlayer(m.white.username)
                var blackPlayerOpt : Option[Profile] = getPlayer(m.black.username)

                while(it.hasNext && whitePlayerOpt.isEmpty || blackPlayerOpt.isEmpty){
                  m = it.next()
                  whitePlayerOpt = getPlayer(m.white.username)
                  blackPlayerOpt = getPlayer(m.black.username)
                }

                if (whitePlayerOpt.isDefined && blackPlayerOpt.isDefined){
                  val whitePlayer = whitePlayerOpt.get
                  val blackPlayer = blackPlayerOpt.get
                  var l = List[Profile]()

                  if (!s.contains(whitePlayer)) l = whitePlayer :: l
                  if (!s.contains(blackPlayer)) l = blackPlayer :: l

                  Some(((m, l), (s, it)))
                }
                else{
                  None
                }

              }
          })
          .take(100)
          .foreach(v => {// Writing to disk
            // E/S
            // write match
            val m = v._1
            matchesFile.write(m.toJson.toString() + "\n")
            // write player/s
            val l = v._2
            l.foreach(p => playersFile.write(p.toJson.toString() + "\n"))
          })


  }

}
