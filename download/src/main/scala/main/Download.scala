package main

import caseapp.{CaseApp, RemainingArgs}
import io.Io
import manager.Manager

object Download extends CaseApp[ArgumentsDownload] {

  def run(args: ArgumentsDownload, r: RemainingArgs): Unit = {
    val io = new Io(args.matchesFile, args.playersFile)
    Manager()
      .take(args.number)
      .foreach(io.write)
  }

}