package com.peknight.fs2

import cats.effect.*
import com.peknight.fs2.io.archivers
import fs2.Pipe
import fs2.io.file.{Files, Path}
import org.apache.commons.compress.archivers.tar.{TarArchiveInputStream, TarArchiveOutputStream, TarArchiveEntry as ApacheTarArchiveEntry}

package object tar:
  def readAll[F[_]: {Sync, Files}]: Pipe[F, Path, TarArchiveEntry[F]] =
    archivers.readAll[F, TarArchiveEntry[F]](TarArchiveEntry.from[F])

  def archive[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, TarArchiveEntry[F], Byte] =
    archivers.archive[F, TarArchiveOutputStream, ApacheTarArchiveEntry, TarArchiveEntry[F]](chunkSize){ outputStream =>
      val tarArchiveOutputStream: TarArchiveOutputStream = TarArchiveOutputStream(outputStream)
      tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
      tarArchiveOutputStream
    }(_.putArchiveEntry(_))(_.closeArchiveEntry())

  def unarchive[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, Byte, TarArchiveEntry[F]] =
    archivers.unarchive[F, TarArchiveInputStream, ApacheTarArchiveEntry, TarArchiveEntry[F]](chunkSize)(
      TarArchiveInputStream(_))(_.getNextEntry)(TarArchiveEntry[F])

  def writeAll[F[_]: {Sync, Files}](target: Path, overwrite: Boolean): Pipe[F, TarArchiveEntry[F], Nothing] =
    archivers.writeAll[F, ApacheTarArchiveEntry, TarArchiveEntry[F]](target, overwrite)
end tar
