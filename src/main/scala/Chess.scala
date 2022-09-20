import org.apache.spark.sql.SparkSession

import scala.util.Try
import spray.json._

case class TitledPlayers(players: List[String])

case class Profile(username: String, // the username of this player
              title: Option[String], // (optional) abbreviation of chess title, if any
              status: String, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff)
              country: String, // API location of this player's country's profile
              followers: Int, // the number of players tracking this player's activity
              is_streamer: Boolean //if the member is a Chess.com streamer
              )

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
  implicit val jsonFormatterProfile: RootJsonFormat[Profile] = jsonFormat6(Profile.apply)
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

    val titles = List("GM", "WG", "IM", "WIM", "FM", "WFM", "CM", "WCM")

    val players : List[List[String]]=
      for {
        title <- titles
      } yield Try(requests.get("https://api.chess.com/pub/titled/" + title).text.parseJson.convertTo[TitledPlayers].players).toOption
      .toList
      .flatten
      .sorted(Ordering[String].reverse)
      .take(5)

    val titled_players : List[String] = players.flatten

//    println(titled_players)

    // 2.- Get players profile

    val profiles : List[Option[Profile]] =
      for {
        name <- titled_players
      } yield Try(requests.get("https://api.chess.com/pub/player/" + name).text.parseJson.convertTo[Profile]).toOption

    var players_profile = profiles.flatten

//    println(players_profile)

    // 3.- Get players tournaments

    val tournaments_from_player : List[Option[List[PlayerTournamentNode]]] = for {
      name <- titled_players
    } yield Try(requests.get("https://api.chess.com/pub/player/" + name + "/tournaments").text.parseJson.convertTo[PlayerTournament].finished).toOption

    val players_tournaments : List[String]= tournaments_from_player
      .flatten
      .take(5)
      .flatten
      .map(t => t.id)

//    println(players_tournaments)

    // 4.- Get tournaments and rounds

    val tournaments : List[Option[List[String]]]= for {
      url <- players_tournaments
    } yield Try(requests.get(url).text.parseJson.convertTo[Tournament].rounds).toOption

    val rounds: List[String]= tournaments.flatten.map(l => l.head) // solamente la primera ronda del torneo

//    println(rounds)

    // 5.- Get rounds and groups

    val rounds_tournaments : List[Option[List[String]]]= for {
      url <- rounds
    } yield Try(requests.get(url).text.parseJson.convertTo[Round].groups).toOption

    val groups: List[String]= rounds_tournaments.flatten.map(l => l.head) // solamente el primer grupo que sale

//    println(groups)

    // 6.- Get matches

    val matches : List[Option[Game]] = for {
      url <- groups
    } yield Try(requests.get(url).text.parseJson.convertTo[Game]).toOption

//    println(matches)

    val player_matches : List[(String, String, Option[String])] =
        matches
          .flatten
          .flatMap(g => g.games.take(3))
          .map(n => (n.white.username, n.black.username, n.eco))

    println(player_matches)

    // 7.- Fill list with player's profiles with players from matches info

    // Nota: Solo agregar jugadores que no estén en profile_players (Corregir)

    val titled_players_from_matches : List[Option[Profile]] = for {
      name <- matches
                .flatten
                .flatMap(g => g.players)
                .map(p => p.username)
    } yield Try(requests.get("https://api.chess.com/pub/player/" + name).text.parseJson.convertTo[Profile]).toOption

    players_profile = titled_players_from_matches.flatten ++ players_profile

    println(players_profile) // 0:40

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
