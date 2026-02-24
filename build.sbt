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
    fs2Tar.jvm,
    fs2Tar.js,
    fs2Zip.jvm,
    fs2Zip.js,
    fs2Xz.jvm,
    fs2Xz.js,
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

lazy val fs2Tar = (crossProject(JVMPlatform, JSPlatform) in file("fs2-tar"))
  .dependsOn(fs2IO)
  .settings(name := "fs2-tar")
  .jvmSettings(libraryDependencies ++= jvmDependencies(
    apache.commons.compress
  ))
  .settings(crossTestDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))

lazy val fs2Zip = (crossProject(JVMPlatform, JSPlatform) in file("fs2-zip"))
  .dependsOn(fs2IO)
  .settings(name := "fs2-zip")
  .jvmSettings(libraryDependencies ++= jvmDependencies(
    apache.commons.compress
  ))
  .settings(crossTestDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))

lazy val fs2Xz = (crossProject(JVMPlatform, JSPlatform) in file("fs2-xz"))
  .dependsOn(
    fs2IO,
    fs2Tar % Test,
  )
  .settings(name := "fs2-xz")
  .jvmSettings(libraryDependencies ++= jvmDependencies(
    apache.commons.compress,
    tukaani.xz,
  ))
  .settings(crossTestDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))
