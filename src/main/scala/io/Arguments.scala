package io

case class Arguments(
  number: Int = 100, // number of matches to be downloaded
  matchesFile: String = "matches.json", // name of file where matches are stored
  playersFile: String = "players.json"// name of file where players are stored
)
