package io

import data.{Match, Profile}
import spray.json._
import manager.Parser._

import java.io.FileWriter

class Io (val matchesFileName: String, val playersFileName: String){

  val matchesFile = new FileWriter(matchesFileName)
  val playersFile = new FileWriter(playersFileName)

  def write(v: Tuple2[Match, List[Profile]]) : Unit = {
    val m = v._1
    matchesFile.write(m.toJson.toString() + "\n")
    val l = v._2
    l.foreach(p => playersFile.write(p.toJson.toString() + "\n"))
  }

  def close() : Unit = {
    matchesFile.close()
    playersFile.close()
  }

}
