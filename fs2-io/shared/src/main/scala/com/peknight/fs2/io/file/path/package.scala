package com.peknight.fs2.io.file

import fs2.io.file.Path

package object path:
  val opt: Path = Path("/opt")
  val varLog: Path = Path("/var/log")
  val bin: Path = Path("bin")
  val conf: Path = Path("conf")
  val data: Path = Path("data")
  val lib: Path = Path("lib")
  val logs: Path = Path("logs")
  val certs: Path = Path("certs")

  val timezone: Path = Path("/etc/timezone")
  val localtime: Path = Path("/etc/localtime")
end path
