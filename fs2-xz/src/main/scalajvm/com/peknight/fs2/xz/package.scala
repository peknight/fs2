package com.peknight.fs2

import cats.effect.{Async, Resource, Sync}
import cats.syntax.applicative.*
import fs2.io.{readInputStream, readOutputStream, toInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import org.apache.commons.compress.compressors.xz.{XZCompressorInputStream, XZCompressorOutputStream}
import org.tukaani.xz.LZMA2Options

import java.io.{BufferedInputStream, BufferedOutputStream}

package object xz:
  def compress[F[_]: Async](preset: Int = 6, chunkSize: Int = 1024 * 32): Pipe[F, Byte, Byte] =
    in => readOutputStream[F](chunkSize)(outputStream => Resource
      .fromAutoCloseable[F, XZCompressorOutputStream](Sync[F].delay(
        XZCompressorOutputStream.builder()
          .setOutputStream(new BufferedOutputStream(outputStream, chunkSize))
          .setLzma2Options(new LZMA2Options(if preset < 0 || preset > 9 then 6 else preset)).get()))
      .use(outputStream => in.through(writeOutputStream[F](outputStream.pure[F], false)).compile.drain)
    )

  def decompress[F[_]: Async](chunkSize: Int = 1024 * 32): Pipe[F, Byte, Byte] =
    _.through(toInputStream)
      .flatMap(inputStream => Stream.fromAutoCloseable(Sync[F].delay(
        new XZCompressorInputStream(new BufferedInputStream(inputStream, chunkSize)))))
      .flatMap(inputStream => readInputStream[F](inputStream.pure[F], chunkSize, true))
end xz
