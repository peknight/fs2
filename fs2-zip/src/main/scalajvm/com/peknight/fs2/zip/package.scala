package com.peknight.fs2

import cats.effect.{Async, Sync}
import com.peknight.fs2.io.archivers
import fs2.Pipe
import fs2.io.file.{Files, Path}
import org.apache.commons.compress.archivers.zip.{ZipArchiveInputStream, ZipArchiveOutputStream, ZipArchiveEntry as ApacheZipArchiveEntry}

package object zip:
  def readAll[F[_]: {Sync, Files}]: Pipe[F, Path, ZipArchiveEntry[F]] =
    archivers.readAll[F, ZipArchiveEntry[F]](ZipArchiveEntry.from[F])

  def archive[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, ZipArchiveEntry[F], Byte] =
    archivers.archive[F, ZipArchiveOutputStream, ApacheZipArchiveEntry, ZipArchiveEntry[F]](chunkSize)(
      ZipArchiveOutputStream(_))(_.putArchiveEntry(_))(_.closeArchiveEntry())

  def unarchive[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, Byte, ZipArchiveEntry[F]] =
    archivers.unarchive[F, ZipArchiveInputStream, ApacheZipArchiveEntry, ZipArchiveEntry[F]](chunkSize)(
      ZipArchiveInputStream(_))(_.getNextEntry)(ZipArchiveEntry[F])

  def writeAll[F[_]: {Sync, Files}](target: Path, overwrite: Boolean): Pipe[F, ZipArchiveEntry[F], Nothing] =
    archivers.writeAll[F, ApacheZipArchiveEntry, ZipArchiveEntry[F]](target, overwrite)
end zip
