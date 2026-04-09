package com.peknight.fs2.io.file

import cats.effect.Sync
import fs2.Stream
import fs2.io.file.{Files, Path}

import java.nio.file.NoSuchFileException

package object path:
  val Root: Path = Path("/")

  val etc: Path = Path("etc")
  val opt: Path = Path("opt")
  val usr: Path = Path("usr")
  val `var`: Path = Path("var")

  val apps: Path = Path("apps")
  val bin: Path = Path("bin")
  val certs: Path = Path("certs")
  val conf: Path = Path("conf")
  val data: Path = Path("data")
  val docker: Path = Path("docker")
  val lib: Path = Path("lib")
  val logs: Path = Path("logs")
  val plugins: Path = Path("plugins")
  val projects: Path = Path("projects")
  val root: Path = Path("root")
  val sbin: Path = Path("sbin")
  val services: Path = Path("services")
  val share: Path = Path("share")

  val `.local`: Path = Path(".local")

  val usrLocal: Path = Root / usr / "local"
  val varLib: Path = Root / `var` / lib
  val varLog: Path = Root / `var` / "log"
  val etcTimezone: Path = Root / etc / "timezone"
  val etcLocaltime: Path = Root / etc / "localtime"

  def recursive[F[_]: {Sync, Files}](path: Path): Stream[F, Path] =
    for
      exists <- Stream.eval(Files[F].exists(path))
      path <- if exists then Stream.emit[F, Path](path) else Stream.raiseError[F](NoSuchFileException(path.toString))
      directory <- Stream.eval(Files[F].isDirectory(path))
      path <- Stream.emit[F, Path](path) ++ (if directory then Files[F].list(path).flatMap(recursive[F]) else Stream.empty)
    yield
      path
end path
