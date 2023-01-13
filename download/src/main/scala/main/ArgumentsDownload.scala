package main

case class ArgumentsDownload(
                              number: Int = 100,
                              matchesFile: String = "matches.json",
                              playersFile: String = "players.json"
                            )