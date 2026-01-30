package com.peknight.fs2.io.archivers

import cats.effect.Sync
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import fs2.Stream
import fs2.io.file.{Files, Path, PosixPermissions}

trait ArchiveEntry[F[_], Entry]:
  def entry: Entry
  def content: Stream[F, Byte]
  def name: Path
  def permissions: Option[PosixPermissions]
  def isDirectory(using Sync[F]): F[Boolean]
  def isLink(using Sync[F]): F[Boolean]
end ArchiveEntry
object ArchiveEntry:
  def from[F[_]: {Sync, Files}, Entry, Archive <: ArchiveEntry[F, Entry]](path: Path, name: Path)
                                                                         (entryF: (Path, Path, PosixPermissions) => Entry)
                                                                         (archiveEntryF: (Entry, Stream[F, Byte]) => Archive)
  : F[Archive] =
    for
      directory <- Files[F].isDirectory(path)
      permissions <- Files[F].getPosixPermissions(path)
      entry <- Sync[F].blocking(entryF(path, name, permissions))
    yield
      archiveEntryF(entry, if directory then Stream.empty else Files[F].readAll(path))
end ArchiveEntry
