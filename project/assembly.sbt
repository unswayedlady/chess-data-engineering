addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))