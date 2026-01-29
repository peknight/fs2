package com.peknight.fs2.tar

import cats.effect.Sync
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import fs2.Stream
import fs2.io.file.{Files, Path, PosixPermissions}
import org.apache.commons.compress.archivers.tar.TarArchiveEntry as ApacheTarArchiveEntry

case class TarArchiveEntry[F[_]](entry: ApacheTarArchiveEntry, content: Stream[F, Byte]):
  val name: Path = Path(entry.getName)
  val permissions: Option[PosixPermissions] = Option(entry.getMode).flatMap(PosixPermissions.fromInt)
  def isDirectory(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isDirectory)
  def isSymbolicLink(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isSymbolicLink)
  def isLink(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isLink)
end TarArchiveEntry
object TarArchiveEntry:
  def from[F[_]: {Sync, Files}](path: Path, name: Path): F[TarArchiveEntry[F]] =
    for
      directory <- Files[F].isDirectory(path)
      permissions <- Files[F].getPosixPermissions(path)
      entry <- Sync[F].blocking {
        val entry = new ApacheTarArchiveEntry(path.toNioPath.toFile, name.toString)
        entry.setMode(permissions.value)
        entry
      }
    yield
      TarArchiveEntry(entry, if directory then Stream.empty else Files[F].readAll(path))
end TarArchiveEntry
