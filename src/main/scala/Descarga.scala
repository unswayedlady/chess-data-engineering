import org.apache.spark.sql.SparkSession
import spray.json._

import java.io.FileWriter
import java.sql.Timestamp

object Descarga {

  case class TitledPlayers(players: List[String])

  case class Profile(player_id: Long, // the non-changing Chess.com ID of this player
                     username: String, // the username of this player
                     title: Option[String], // (optional) abbreviation of chess title, if any
                     status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
                     country: String, // API location of this player's country's profile
                     followers: Int, // the number of players tracking this player's activity
                     is_streamer: Boolean, //if the member is a Chess.com streamer
                     joined: Timestamp// timestamp of registration on Chess.com
                    ) {
    override def equals(obj: Any): Boolean = {
      obj match{
        case Profile => this.player_id.equals(obj.asInstanceOf[Profile].player_id)
        case _ => false
      }

    }
  }

  case class PlayerTournamentNode(id: String) // id of tournament
  case class PlayerTournament(finished: List[PlayerTournamentNode]) // list of tournaments
  // played by user

  case class Tournament(rounds: List[String]) // link of rounds of tournaments

  case class Round(groups: List[String]) // link of groups of rounds of tournaments

  case class Player(username: String) // player username

  case class GameNode(white: Player, //white player
                      black: Player, //black player
                      eco: Option[String], // opening
                      time_class: String, // time-per-move grouping, used for ratings
                     )

  case class Game(games: List[GameNode])

  object myJsonProtocol extends DefaultJsonProtocol{

    implicit object TimestampJsonFormat extends JsonFormat[Timestamp] {
      def write(x: Timestamp): JsNumber = JsNumber(x.getTime)
      def read(value: JsValue): Timestamp = value match {
        case JsNumber(x) => new Timestamp(x.longValue)
        case x => deserializationError("Expected Timestamp as JsNumber, but got " + x)
      }
    }

    implicit val jsonFormatterTitledPlayers: RootJsonFormat[TitledPlayers] = jsonFormat1(TitledPlayers.apply)
    implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat8(Profile.apply)
    implicit val jsonFormatterPlayerTournamentNode: RootJsonFormat[PlayerTournamentNode] = jsonFormat(PlayerTournamentNode, "@id")
    implicit val jsonFormatterPlayerTournament: RootJsonFormat[PlayerTournament] = jsonFormat1(PlayerTournament.apply)
    implicit val jsonFormatterTournament: RootJsonFormat[Tournament] = jsonFormat1(Tournament.apply)
    implicit val jsonFormatterRounds: RootJsonFormat[Round] = jsonFormat1(Round.apply)
    implicit val jsonFormatterPlayer: RootJsonFormat[Player] = jsonFormat1(Player.apply)
    implicit val jsonFormatterGameNode: RootJsonFormat[GameNode] = jsonFormat4(GameNode.apply)
    implicit val jsonFormatterGame: RootJsonFormat[Game] = jsonFormat1(Game.apply)
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

    def getPlayersId(title: String): Iterator[String] = {
      requests
        .get("https://api.chess.com/pub/titled/" + title)
        .text
        .parseJson
        .convertTo[TitledPlayers]
        .players
        .iterator
    }

    // 2.- Get players tournaments id method

    def getPlayerTournamentsId(playerId: String): Iterator[String] = {
      requests
        .get("https://api.chess.com/pub/player/" + playerId + "/tournaments")
        .text
        .parseJson
        .convertTo[PlayerTournament]
        .finished
        .map(_.id)
        .iterator
    }

    // 3.- Get rounds id method

    def getRoundsId(tournamentId: String) : Iterator[String] = {
      requests
        .get(tournamentId)
        .text()
        .parseJson
        .convertTo[Tournament]
        .rounds
        .iterator
    }

    // 4.- Get groups id method

    def getGroupsId(roundId: String) : Iterator[String] = {
      requests
        .get(roundId)
        .text()
        .parseJson
        .convertTo[Round]
        .groups
        .iterator
    }

    // 5.- Get match method

    def getMatch(groupId: String) : Iterator[GameNode] = {
      requests
        .get(groupId)
        .text()
        .parseJson
        .convertTo[Game]
        .games
        .iterator
    }

    // Store matches (edges) + profiles (vertices) in a file

    var numberRequests = 0
    val matchesFile = new FileWriter("matches.json")
    val profilesFile = new FileWriter("profiles.json")

    List("GM", "WGM", "IM", "WIM", "FM", "WFM", "CM", "WCM")
      .iterator
      .flatMap(getPlayersId)
      .flatMap(getPlayerTournamentsId)
      .flatMap(getRoundsId)
      .flatMap(getGroupsId)
      .flatMap(getMatch)
      .take(500)
      .foreach(g => {
        numberRequests+=1
        if (numberRequests % 100 == 0){
          println("We had processed so far " +  numberRequests +  " requests...")
        }
        matchesFile.write(g.toJson.toString()+"\n")// write edge
        profilesFile.write(requests
          .get("https://api.chess.com/pub/player/" + g.white.username)
          .text
          .parseJson
          .convertTo[Profile].toJson.toString()+"\n")// vertex 1
        profilesFile.write(requests
          .get("https://api.chess.com/pub/player/" + g.black.username)
          .text
          .parseJson
          .convertTo[Profile].toJson.toString()+"\n")// vertex 2
      })


  }

}
