import com.peknight.build.gav
import com.peknight.build.gav.*
import com.peknight.build.sbt.*

commonSettings

lazy val fs2 = (project in file("."))
  .settings(name := "fs2")
  .aggregate(
    fs2Core.jvm,
    fs2Core.js,
    fs2IO.jvm,
    fs2IO.js,
  )

lazy val fs2Core = (crossProject(JVMPlatform, JSPlatform) in file("fs2-core"))
  .settings(name := "fs2-core")
  .settings(crossDependencies(
    gav.fs2,
    peknight.cats,
  ))

lazy val fs2IO = (crossProject(JVMPlatform, JSPlatform) in file("fs2-io"))
  .settings(name := "fs2-io")
  .settings(crossDependencies(gav.fs2.io))
