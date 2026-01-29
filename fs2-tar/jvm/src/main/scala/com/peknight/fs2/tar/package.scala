package com.peknight.fs2

import cats.effect.*
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
import org.apache.commons.compress.archivers.tar.{TarArchiveInputStream, TarArchiveOutputStream}

package object tar:
  def archive[F[_]: {Sync, Files}]: Pipe[F, Path, TarArchiveEntry[F]] =
    _.flatMap(path => Stream.eval[F, Path](Files[F].realPath(path)))
      .flatMap(realPath => recursive[F](realPath).map(p => (p, realPath.parent.getOrElse(Root).relativize(p))))
      .evalMap(TarArchiveEntry.from[F])

  def readAll[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, TarArchiveEntry[F], Byte] = in =>
    readOutputStream[F](chunkSize)(outputStream => Resource
      .fromAutoCloseable[F, TarArchiveOutputStream](Sync[F].delay(TarArchiveOutputStream(outputStream)))
      .use(tarArchiveOutputStream => in
        .flatMap(tarArchiveEntry => Stream
          .resource(Resource.make(Sync[F].blocking(tarArchiveOutputStream.putArchiveEntry(tarArchiveEntry.entry)))(
            _ => Sync[F].blocking(tarArchiveOutputStream.closeArchiveEntry())))
          .flatMap(_ => tarArchiveEntry.content.through(writeOutputStream(tarArchiveOutputStream.pure[F], false))))
        .compile.drain
      )
    )

  def unarchive[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, Byte, TarArchiveEntry[F]] = in =>
    for
      inputStream <- in.through(toInputStream[F])
      tarArchiveInputStream <- Stream.fromAutoCloseable(Sync[F].delay(TarArchiveInputStream(inputStream)))
      tarArchiveEntry <- Stream.unfoldEval[F, Unit, Stream[F, TarArchiveEntry[F]]](())(_ => Sync[F]
          .blocking(Option(tarArchiveInputStream.getNextEntry))
          .flatMap(_.fold(none[(Stream[F, TarArchiveEntry[F]], Unit)].pure[F]) { tarArchiveEntry =>
            for
              deferred <- Deferred[F, Unit]
            yield
              val content: Stream[F, Byte] = readInputStream[F](tarArchiveInputStream.pure[F], chunkSize, false) ++
                Stream.exec(deferred.complete(()).void)
              val tarArchiveEntries: Stream[F, TarArchiveEntry[F]] =
                Stream.emit[F, TarArchiveEntry[F]](TarArchiveEntry[F](tarArchiveEntry, content)) ++
                  Stream.exec(deferred.get)
              (tarArchiveEntries, ()).some
          }))
        .flatten
    yield
      tarArchiveEntry

  private def setPosixPermissions[F[_]: {Applicative, Files}](tarArchiveEntry: TarArchiveEntry[F], path: Path): F[Unit] =
    tarArchiveEntry.permissions.fold(().pure[F])(permissions => Files[F].setPosixPermissions(path, permissions))

  def writeAll[F[_]: {Sync, Files}](target: Path, overwrite: Boolean): Pipe[F, TarArchiveEntry[F], Nothing] =
    _.evalMap[F, Unit] { tarArchiveEntry =>
      val path: Path = target / tarArchiveEntry.name
      Monad[F].ifM[Unit](tarArchiveEntry.isDirectory)(
        Files[F].createDirectories(path)
          .flatMap(_ => tarArchiveEntry.content.compile.drain)
          .flatMap(_ => setPosixPermissions[F](tarArchiveEntry, path)),
        Monad[F].ifM[Unit](tarArchiveEntry.isSymbolicLink
          .flatMap(symbolicLink => if symbolicLink then true.pure[F] else tarArchiveEntry.isLink))(
          tarArchiveEntry.content.compile.drain,
          Monad[F].ifM[Unit](if overwrite then false.pure[F] else Files[F].exists(path))(
            tarArchiveEntry.content.compile.drain,
            path.createParentDirectories[F]()
              .flatMap(_ => tarArchiveEntry.content.through(Files[F].writeAll(path)).compile.drain)
              .flatMap(_ => setPosixPermissions[F](tarArchiveEntry, path)),
          )
        )
      )
    }.drain
end tar
