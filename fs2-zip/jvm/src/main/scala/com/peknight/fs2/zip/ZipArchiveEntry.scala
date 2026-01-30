package com.peknight.fs2.zip

import cats.effect.Sync
import com.peknight.fs2.io.archivers.ArchiveEntry
import fs2.Stream
import fs2.io.file.{Files, Path, PosixPermissions}
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry as ApacheZipArchiveEntry

case class ZipArchiveEntry[F[_]](entry: ApacheZipArchiveEntry, content: Stream[F, Byte])
  extends ArchiveEntry[F, ApacheZipArchiveEntry]:
  val name: Path = Path(entry.getName)
  val permissions: Option[PosixPermissions] = Option(entry.getUnixMode)
    .filter(_ > 0)
    .flatMap(PosixPermissions.fromInt)
  def isDirectory(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isDirectory)
  def isLink(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isUnixSymlink)
end ZipArchiveEntry
object ZipArchiveEntry:
  def from[F[_]: {Sync, Files}](path: Path, name: Path): F[ZipArchiveEntry[F]] =
    ArchiveEntry.from[F, ApacheZipArchiveEntry, ZipArchiveEntry[F]](path, name) { (path, name, permissions) =>
      val entry = new ApacheZipArchiveEntry(path.toNioPath.toFile, name.toString)
      entry.setUnixMode(permissions.value)
      entry
    }(ZipArchiveEntry[F])
end ZipArchiveEntry
