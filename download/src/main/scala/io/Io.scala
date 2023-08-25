package io

import data.{Match, Profile}
import manager.Parser._
import spray.json._

import java.io.FileWriter

class Io (val matchesFileName: String, val playersFileName: String){

  val matchesFile = new FileWriter(matchesFileName)
  val playersFile = new FileWriter(playersFileName)

  def write(infoDataset: (Match, List[Profile])) : Unit = {
    val matchInfo = infoDataset._1
    matchesFile.write(matchInfo.toJson.toString() + "\n")
    val players = infoDataset._2
    players.foreach(player => playersFile.write(player.toJson.toString() + "\n"))
  }

  def close() : Unit = {
    matchesFile.close()
    playersFile.close()
  }

}
