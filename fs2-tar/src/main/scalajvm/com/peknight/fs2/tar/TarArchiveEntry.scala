package com.peknight.fs2.tar

import cats.effect.Sync
import com.peknight.fs2.io.archivers.ArchiveEntry
import fs2.Stream
import fs2.io.file.{Files, Path, PosixPermissions}
import org.apache.commons.compress.archivers.tar.TarArchiveEntry as ApacheTarArchiveEntry

case class TarArchiveEntry[F[_]](entry: ApacheTarArchiveEntry, content: Stream[F, Byte])
  extends ArchiveEntry[F, ApacheTarArchiveEntry]:
  val name: Path = Path(entry.getName)
  val permissions: Option[PosixPermissions] = Option(entry.getMode)
    .filter(_ > 0)
    .flatMap(PosixPermissions.fromInt)
  def isDirectory(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isDirectory)
  def isLink(using Sync[F]): F[Boolean] = Sync[F].blocking(entry.isSymbolicLink || entry.isLink)
  def copy(name: Path): TarArchiveEntry[F] =
    val next = new ApacheTarArchiveEntry(name.toString, entry.getLinkFlag)
    next.setCreationTime(entry.getCreationTime)
    if entry.getDataOffset >= 0 then next.setDataOffset(entry.getDataOffset) else ()
    next.setDevMajor(entry.getDevMajor)
    next.setDevMinor(entry.getDevMinor)
    next.setGroupId(entry.getLongGroupId)
    next.setGroupName(entry.getGroupName)
    next.setLastAccessTime(entry.getLastAccessTime)
    next.setLastModifiedTime(entry.getLastModifiedTime)
    next.setLinkName(entry.getLinkName)
    next.setMode(entry.getMode)
    next.setModTime(entry.getModTime)
    next.setSize(entry.getSize)
    next.setSparseHeaders(entry.getSparseHeaders)
    next.setStatusChangeTime(entry.getStatusChangeTime)
    next.setUserId(entry.getLongUserId)
    next.setUserName(entry.getUserName)
    TarArchiveEntry[F](next, content)
  end copy
end TarArchiveEntry
object TarArchiveEntry:
  def from[F[_]: {Sync, Files}](path: Path, name: Path): F[TarArchiveEntry[F]] =
    ArchiveEntry.from[F, ApacheTarArchiveEntry, TarArchiveEntry[F]](path, name) { (path, name, permissions) =>
      val entry = new ApacheTarArchiveEntry(path.toNioPath.toFile, name.toString)
      entry.setMode(permissions.value)
      entry
    }(TarArchiveEntry[F])
end TarArchiveEntry
