package com.peknight.fs2.io

import cats.effect.{Async, Deferred, Resource, Sync}
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.{Applicative, Monad}
import com.peknight.fs2.io.file.path.{Root, recursive}
import com.peknight.fs2.io.syntax.path.createParentDirectories
import fs2.io.file.{Files, Path}
import fs2.io.{readInputStream, readOutputStream, toInputStream, writeOutputStream}
import fs2.{Pipe, Stream}

import java.io.{BufferedInputStream, InputStream, OutputStream}

package object archivers:
  def readAll[F[_]: {Sync, Files}, Archive <: ArchiveEntry[F, ?]](f: (Path, Path) => F[Archive])
  : Pipe[F, Path, Archive] =
    _.flatMap(path => Stream.eval[F, Path](Files[F].realPath(path)))
      .flatMap(realPath => recursive[F](realPath).map(p => (p, realPath.parent.getOrElse(Root).relativize(p))))
      .evalMap((path, name) => f(path, name))

  def archive[F[_]: Async, ArchiveOutputStream <: OutputStream, Entry, Archive <: ArchiveEntry[F, Entry]]
             (chunkSize: Int = 1024 * 32)
             (streamF: OutputStream => ArchiveOutputStream)
             (putArchiveEntry: (ArchiveOutputStream, Entry) => Unit)
             (closeArchiveEntry: ArchiveOutputStream => Unit)
  : Pipe[F, Archive, Byte] = in =>
    readOutputStream[F](chunkSize)(outputStream => Resource
      .fromAutoCloseable[F, ArchiveOutputStream](Sync[F].delay(streamF(outputStream)))
      .use(archiveOutputStream => in
        .flatMap(archiveEntry => Stream
          .resource(Resource.make(Sync[F].blocking(putArchiveEntry(archiveOutputStream, archiveEntry.entry)))(
            _ => Sync[F].blocking(closeArchiveEntry(archiveOutputStream))))
          .flatMap(_ => archiveEntry.content.through(writeOutputStream(archiveOutputStream.pure[F], false))))
        .compile.drain
      )
    )

  def unarchive[F[_]: Async, ArchiveInputStream <: InputStream, Entry, Archive <: ArchiveEntry[F, Entry]]
               (chunkSize: Int = 1024 * 32)
               (streamF: InputStream => ArchiveInputStream)
               (getNextEntry: ArchiveInputStream => Entry)
               (archiveF: (Entry, Stream[F, Byte]) => Archive)
  : Pipe[F, Byte, Archive] = in =>
    for
      inputStream <- in.through(toInputStream[F])
      archiveInputStream <- Stream.fromAutoCloseable(Sync[F]
        .delay(streamF(BufferedInputStream(inputStream, chunkSize))))
      archiveEntry <- Stream.unfoldEval[F, Unit, Stream[F, Archive]](())(
          _ => Sync[F].blocking(Option(getNextEntry(archiveInputStream))).flatMap {
            case Some(entry) => Deferred[F, Unit].map { deferred =>
              val content: Stream[F, Byte] = readInputStream[F](archiveInputStream.pure[F], chunkSize, false) ++
                Stream.exec(deferred.complete(()).void)
              val stream: Stream[F, Archive] =
                Stream.emit[F, Archive](archiveF(entry, content)) ++ Stream.exec(deferred.get)
              (stream, ()).some
            }
            case _ => none[(Stream[F, Archive], Unit)].pure[F]
          }
        )
        .flatten
    yield
      archiveEntry

  private def setPosixPermissions[F[_]: {Applicative, Files}, Entry, Archive <: ArchiveEntry[F, Entry]]
                                 (archiveEntry: Archive, path: Path): F[Unit] =
    archiveEntry.permissions.fold(().pure[F])(permissions => Files[F].setPosixPermissions(path, permissions))

  def writeAll[F[_]: {Sync, Files}, Entry, Archive <: ArchiveEntry[F, Entry]](target: Path, overwrite: Boolean): Pipe[F, Archive, Nothing] =
    _.evalMap[F, Unit] { archiveEntry =>
      val path: Path = target / archiveEntry.name
      Monad[F].ifM[Unit](archiveEntry.isDirectory)(
        Files[F].createDirectories(path)
          .flatMap(_ => archiveEntry.content.compile.drain)
          .flatMap(_ => setPosixPermissions[F, Entry, Archive](archiveEntry, path)),
        Monad[F].ifM[Unit](archiveEntry.isLink)(
          archiveEntry.content.compile.drain,
          Monad[F].ifM[Unit](if overwrite then false.pure[F] else Files[F].exists(path))(
            archiveEntry.content.compile.drain,
            path.createParentDirectories[F]()
              .flatMap(_ => archiveEntry.content.through(Files[F].writeAll(path)).compile.drain)
              .flatMap(_ => setPosixPermissions[F, Entry, Archive](archiveEntry, path)),
          )
        )
      )
    }.drain
end archivers
