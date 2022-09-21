import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.Try
import spray.json._
import org.graphframes.GraphFrame

case class TitledPlayers(players: List[String])

case class Profile(player_id: Long, // the non-changing Chess.com ID of this player
                    username: String, // the username of this player
              title: Option[String], // (optional) abbreviation of chess title, if any
              status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
              country: String, // API location of this player's country's profile
              followers: Int, // the number of players tracking this player's activity
              is_streamer: Boolean //if the member is a Chess.com streamer
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
                    eco:Option[String] // opening
                   )
case class Game(games: List[GameNode], players: List[Player])

object myJsonProtocol extends DefaultJsonProtocol{
  implicit val jsonFormatterTitledPlayers: RootJsonFormat[TitledPlayers] = jsonFormat1(TitledPlayers.apply)
  implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat7(Profile.apply)
  implicit val jsonFormatterPlayerTournamentNode: RootJsonFormat[PlayerTournamentNode] = jsonFormat(PlayerTournamentNode, "@id")
  implicit val jsonFormatterPlayerTournament: RootJsonFormat[PlayerTournament] = jsonFormat1(PlayerTournament.apply)
  implicit val jsonFormatterTournament: RootJsonFormat[Tournament] = jsonFormat1(Tournament.apply)
  implicit val jsonFormatterRounds: RootJsonFormat[Round] = jsonFormat1(Round.apply)
  implicit val jsonFormatterPlayer: RootJsonFormat[Player] = jsonFormat1(Player.apply)
  implicit val jsonFormatterGameNode: RootJsonFormat[GameNode] = jsonFormat3(GameNode.apply)
  implicit val jsonFormatterGame: RootJsonFormat[Game] = jsonFormat2(Game.apply)
}

object Chess {

  def main(args: Array[String]): Unit ={

    import myJsonProtocol._

    val spark = SparkSession
      .builder
      .appName("Chess")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1.- Get titled players

    val titles = List("GM", "WGM", "IM", "WIM", "FM", "WFM", "CM", "WCM")

    val titled_players : List[String] =
     titles.flatMap { title =>
       Try(requests.get("https://api.chess.com/pub/titled/" + title).text.parseJson.convertTo[TitledPlayers].players)
         .fold(_ => List(), identity)
         .take(5)
     }

    println(titled_players) // 40
    println(titled_players.size)

    // 2.- Get profiles

    var profiles : List[Profile] =
      titled_players.flatMap { name =>
        Try(requests.get("https://api.chess.com/pub/player/" + name).text.parseJson.convertTo[Profile])
          .fold(_ => List(), profile => List(profile))
      }

    println(profiles)
    println(profiles.size) // 40 vértices

    // 3.- Get tournaments

    val tournaments : List[String] = titled_players.flatMap {name =>
      Try(requests.get("https://api.chess.com/pub/player/" + name + "/tournaments").text.parseJson.convertTo[PlayerTournament].finished)
        .fold(_ => List(), l => l.take(5))
        .map(_.id)
    }

    println(tournaments)
    println(tournaments.size) // 40 * 5 = 200 | 140

    // 4.- Get rounds

    val rounds : List[String] = tournaments flatMap { url =>
      Try(requests.get(url).text.parseJson.convertTo[Tournament].rounds)
        .fold(_ => List(), l => List(l.head)) // solamente la primera ronda del torneo
    }

    println(rounds)
    println(rounds.size) // 138

    // 5.- Get groups

    val groups : List[String] = rounds flatMap { url =>
      Try(requests.get(url).text.parseJson.convertTo[Round].groups)
        .fold(_ => List(), l => List(l.head)) // solamente el primer grupo que sale
    }

    println(groups)
    println(groups.size) // 138

    // 6.- Get matches

    val matches : List[(String, String, Option[String])] = groups flatMap {url =>
      Try(requests.get(url).text.parseJson.convertTo[Game].games)
      .fold(_ => List(), l=>l.take(3))
      .map(g => (g.white.username, g.black.username, g.eco))
    }

    println(matches)
    println(matches.size) // 373 aristas

    // 7.- Fill profiles list with players from matches info

    profiles = matches.map(m=>
      (Try(requests.get("https://api.chess.com/pub/player/" + m._1).text.parseJson.convertTo[Profile])
        .fold(_ => List(), List(_))
        ,
      Try(requests.get("https://api.chess.com/pub/player/" + m._2).text.parseJson.convertTo[Profile])
        .fold(_ => List(), List(_))
      ))
      .filter(!profiles.contains(_))
      .flatMap(t => t._1 ++ t._2) ++ profiles

    println(profiles)
    println(profiles.size) // 786 nodos

    // 8.- Generate graph

    val vertices = spark.sqlContext.createDataFrame(profiles).toDF("player_id",
      "username",
      "title",
      "status",
      "country",
      "followers",
      "is_streamer")

    val edges = spark.sqlContext.createDataFrame(matches).toDF("white", "black", "eco")

    val playersVertices = vertices
      .withColumnRenamed("username", "id").distinct()

    val gamesEdges = edges
      .withColumnRenamed("white", "src")
      .withColumnRenamed("black", "dst")

    val chessGraph = GraphFrame(playersVertices, gamesEdges)
    chessGraph.cache()

    // 1.- matches donde se jugó la defensa siciliana

    chessGraph
      .edges
      .where(col("eco").contains("Sicilian-Defense"))
      .show(5)


    // -----------------------------------------

// UNFOLD IDEA =>
//
//    val it : Iterator[Response]= Iterator.unfold(Request("https://api.chess.com/pub/player/erik"))(
//      respuesta : Response =>  Some((respuesta, ))
//    )

/*   -> IDEA

    //endpoint del perfil del jugador x
    val url_perfil = "https://api.chess.com/pub/player/" + name
    requests.get(url_perfil)

    //endpoint de los torneos del jugador x
    val url_torneo_jugador = "https://api.chess.com/pub/player/" + name + "/tournaments"
    requests.get(url_torneo_jugador)

    // endpoint de la info. del torneo x
    val url_torneo = requests.get("https://api.chess.com/pub/tournament/50-blitz-429084"
    // se obtiene del campo @id de la request de los torneos del jugador

    //endpoint de las rondas x
    val url_ronda = "https://api.chess.com/pub/tournament/50-blitz-429084/4"
    //se obtiene del campo "rounds" de la request de la info. del torneo

    //endpoint de las partidas x
    val url_partida = "https://api.chess.com/pub/tournament/50-blitz-429084/4/1"
    requests.get(url_partida)
    // se obtiene del campo "groups" de la request de las rondas*/

  }

}
