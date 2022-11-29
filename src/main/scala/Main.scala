import caseapp.{CaseApp, RemainingArgs}
import io.{Arguments, Io}
import manager.Manager

object Main extends CaseApp[Arguments]{

  def run(args: Arguments, r: RemainingArgs): Unit = {

    // Creating io instance

    val io = new Io(args.matchesFile, args.playersFile)

    // Download chess data

    Manager() // get data
    .take(args.number)
    .foreach(io.write) // write to disk*/

  }

}
