import com.peknight.build.gav
import com.peknight.build.gav.*
import com.peknight.build.sbt.*

commonSettings

lazy val fs2 = (project in file("."))
  .settings(name := "fs2")
  .aggregate(fs2Core.projectRefs *)
  .aggregate(fs2IO.projectRefs *)
  .aggregate(fs2Tar.projectRefs *)
  .aggregate(fs2Zip.projectRefs *)
  .aggregate(fs2Xz.projectRefs *)

lazy val fs2Core = (projectMatrix in file("fs2-core"))
  .settings(name := "fs2-core")
  .settings(libraryDependencies ++= dependencies(
    gav.fs2,
    peknight.cats,
  ))
  .jvmPlatform(scalaVersions = Seq(scala.scala3.version))
  .jsPlatform(scalaVersions = Seq(scala.scala3.version))

lazy val fs2IO = (projectMatrix in file("fs2-io"))
  .settings(name := "fs2-io")
  .settings(libraryDependencies ++= dependencies(gav.fs2.io))
  .jvmPlatform(scalaVersions = Seq(scala.scala3.version))
  .jsPlatform(scalaVersions = Seq(scala.scala3.version))

lazy val fs2Tar = (projectMatrix in file("fs2-tar"))
  .dependsOn(fs2IO)
  .settings(name := "fs2-tar")
  .settings(libraryDependencies ++= testDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))
  .jvmPlatform(
    scalaVersions = Seq(scala.scala3.version),
    settings = libraryDependencies ++= jvmDependencies(apache.commons.compress)
  )
  .jsPlatform(scalaVersions = Seq(scala.scala3.version))

lazy val fs2Zip = (projectMatrix in file("fs2-zip"))
  .dependsOn(fs2IO)
  .settings(name := "fs2-zip")
  .settings(libraryDependencies ++= testDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))
  .jvmPlatform(
    scalaVersions = Seq(scala.scala3.version),
    settings = libraryDependencies ++= jvmDependencies(apache.commons.compress)
  )
  .jsPlatform(scalaVersions = Seq(scala.scala3.version))

lazy val fs2Xz = (projectMatrix in file("fs2-xz"))
  .dependsOn(
    fs2IO,
    fs2Tar % Test,
  )
  .settings(name := "fs2-xz")
  .settings(libraryDependencies ++= testDependencies(
    scalaTest.flatSpec,
    typelevel.catsEffect.testingScalaTest
  ))
  .jvmPlatform(
    scalaVersions = Seq(scala.scala3.version),
    settings = libraryDependencies ++= jvmDependencies(
      apache.commons.compress,
      tukaani.xz,
    )
  )
  .jsPlatform(scalaVersions = Seq(scala.scala3.version))
