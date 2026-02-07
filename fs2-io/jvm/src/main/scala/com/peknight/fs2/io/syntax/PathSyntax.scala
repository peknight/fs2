package com.peknight.fs2.io.syntax

import cats.effect.Sync
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.{Applicative, Monad}
import fs2.io.file.*
import fs2.{Compiler, Stream}

import java.io.{FileInputStream, FileOutputStream, FileReader, FileWriter}
import java.nio.file.Paths

trait PathSyntax:
  extension (path: Path)
    def toFileReader[F[_]: Sync]: F[FileReader] = Sync[F].blocking(new FileReader(path.toNioPath.toFile))
    def toFileWriter[F[_]: Sync]: F[FileWriter] = Sync[F].blocking(new FileWriter(path.toNioPath.toFile))
    def toFileInputStream[F[_]: Sync]: F[FileInputStream] =
      Sync[F].blocking(new FileInputStream(path.toNioPath.toFile))
    def toFileOutputStream[F[_]: Sync]: F[FileOutputStream] =
      Sync[F].blocking(new FileOutputStream(path.toNioPath.toFile))
    def createParentDirectories[F[_]: {Applicative, Files}](permissions: Option[Permissions] = None): F[Unit] =
      path.parent.fold(().pure[F])(parent => Files[F].createDirectories(parent, permissions))
    def createFileIfNotExists[F[_]: {Monad, Files}](filePermissions: Option[Permissions] = None,
                                                    directoryPermissions: Option[Permissions] = None): F[Unit] =
      for
        _ <- createParentDirectories[F](directoryPermissions)
        _ <- Monad[F].ifM[Unit](Files[F].exists(path))(().pure[F], Files[F].createFile(path, filePermissions))
      yield
        ()
    def writeFile[F[_]](stream: Stream[F, Byte], flags: Flags = Flags.Write, directoryPermissions: Option[Permissions] = None)
                       (using Monad[F], Files[F], Compiler[F, F]): F[Unit] =
      for
        _ <- createParentDirectories[F](directoryPermissions)
        _ <- stream.through(Files[F].writeAll(path, flags)).compile.drain
      yield
        ()
    def writeFileIfNotExists[F[_]](stream: => Stream[F, Byte], flags: Flags = Flags.Write,
                                   directoryPermissions: Option[Permissions] = None)
                                  (using Monad[F], Files[F], Compiler[F, F]): F[Unit] =
      for
        _ <- createParentDirectories[F](directoryPermissions)
        _ <- Monad[F].ifM[Unit](Files[F].exists(path))(().pure[F], stream.through(Files[F].writeAll(path, flags)).compile.drain)
      yield
        ()
    def addPosixPermission[F[_]: {Monad, Files}](permission: PosixPermission): F[Unit] =
      for
        permissions <- Files[F].getPosixPermissions(path)
        _ <- Files[F].setPosixPermissions(path, permissions.add(permission))
      yield
        ()
  end extension
  extension (path: Path.type)
    def fromResource[F[_]: Sync](resourcePath: String): F[Path] =
      Sync[F].blocking(Path.fromNioPath(Paths.get(getClass.getClassLoader.getResource(resourcePath).toURI)))
  end extension
end PathSyntax
object PathSyntax extends PathSyntax
