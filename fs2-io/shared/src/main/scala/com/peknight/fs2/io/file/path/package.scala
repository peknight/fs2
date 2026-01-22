package com.peknight.fs2.io.file

import fs2.io.file.Path

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
  val projects: Path = Path("projects")
  val services: Path = Path("services")
  val share: Path = Path("share")

  val `.local`: Path = Path(".local")

  val usrLocal: Path = Root / usr / "local"
  val varLib: Path = Root / `var` / "lib"
  val varLog: Path = Root / `var` / "log"
  val etcTimezone: Path = Root / etc / "timezone"
  val etcLocaltime: Path = Root / etc / "localtime"
end path
