package data

case class Match(white: Player, //white player
                 black: Player, //black player
                 eco: Option[String], // opening
                 pgn: String) // current PGN

