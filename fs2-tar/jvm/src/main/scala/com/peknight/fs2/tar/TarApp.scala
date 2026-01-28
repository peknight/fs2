package com.peknight.fs2.tar

import cats.effect.{IO, IOApp, Sync}
import fs2.Stream
import fs2.compression.Compression
import fs2.io.file.{Files, Path}
import fs2.io.toInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

import scala.jdk.CollectionConverters.*

object TarApp extends IOApp.Simple:

  val run: IO[Unit] =
    for
      _ <- Files[IO].readAll(Path("/home/pek/opt/OpenJDK25U-jdk_x64_linux_hotspot_25.0.2_10.tar.gz"))
        .through(Compression[IO].gunzip())
        .evalMap(gunzipResult => gunzipResult.content
          .through(toInputStream[IO])
          .flatMap(inputStream => Stream.fromAutoCloseable(Sync[IO].delay(TarArchiveInputStream(inputStream))))
          .flatMap(tarArchiveInputStream => Stream.fromBlockingIterator[IO](tarArchiveInputStream.iterator().asIterator().asScala, 16))
          .evalTap(tarArchiveEntry => IO.unit)
          .compile.drain)
        .compile
        .drain
    yield
      ()
end TarApp
