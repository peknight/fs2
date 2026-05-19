package com.peknight.fs2.syntax

import cats.effect.*
import cats.effect.std.AtomicCell
import cats.effect.syntax.spawn.*
import cats.syntax.applicative.*
import cats.syntax.applicativeError.*
import cats.syntax.apply.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.{Applicative, Functor}
import com.peknight.fs2.pipe
import fs2.{Chunk, Compiler, Pull, Stream}

import java.util.NoSuchElementException

trait StreamSyntax:
  extension [F[_], I] (stream: Stream[F, I])
    def uncons1(using Functor[F], Compiler[F, F]): F[Option[(I, Stream[F, I])]] =
      stream.pull.uncons1.flatMap(Pull.output1).stream.compile.toList.map(_.headOption.flatten)

    def allocate(f: (Chunk[I], Chunk[I]) => (Chunk[I], Chunk[I])): Stream[F, I] = stream.through(pipe.allocate(f))

    def chunkTimesN(n: Int): Stream[F, I] = stream.through(pipe.chunkTimesN(n))

    def scanChunksInitLast[I2 >: I, O, S](initS: => S)
      (init: (S, Chunk[I2]) => (S, Chunk[O]))(last: (S, Chunk[I2]) => Chunk[O]): Stream[F, O] =
      stream.through(pipe.scanChunksInitLast(initS)(init)(last))

    def mapChunksInitLast[I2 >: I, O](init: Chunk[I2] => Chunk[O])(last: Chunk[I2] => Chunk[O]): Stream[F, O] =
      stream.through(pipe.mapChunksInitLast(init)(last))

    def evalScanChunksOpt[F2[x] >: F[x], I2 >: I, O, S](init: => S)(f: S => Option[Chunk[I2] => F2[(S, Chunk[O])]])
    : Stream[F2, O] = stream.through(pipe.evalScanChunksOpt(init)(f))

    def evalScanChunks[F2[x] >: F[x], I2 >: I, O, S](init: => S)(f: (S, Chunk[I2]) => F2[(S, Chunk[O])])
    : Stream[F2, O] = stream.through(pipe.evalScanChunks(init)(f))

    def evalMapChunks[F2[x] >: F[x], I2 >: I, O](f: Chunk[I2] => F2[Chunk[O]]): Stream[F2, O] =
      stream.through(pipe.evalMapChunks(f))

    def evalTapChunks[F2[x] >: F[x], I2 >: I, O](f: Chunk[I2] => F2[O]): Stream[F2, I] =
      stream.through(pipe.evalTapChunks(f))

    def evalScanChunksInitLast[F2[x] >: F[x] : Applicative, I2 >: I, O, S](initS: => S)
      (init: (S, Chunk[I2]) => F2[(S, Chunk[O])])(last: (S, Chunk[I2]) => F2[Chunk[O]]): Stream[F2, O] =
      stream.through(pipe.evalScanChunksInitLast(initS)(init)(last))

    def evalMapChunksInitLast[F2[x] >: F[x], I2 >: I, O](init: Chunk[I2] => F2[Chunk[O]])
      (last: Chunk[I2] => F2[Chunk[O]]): Stream[F2, O] =
      stream.through(pipe.evalMapChunksInitLast(init)(last))


    def evalTapChunksInitLast[F2[x] >: F[x], I2 >: I, O1, O2](init: Chunk[I2] => F2[O1])(last: Chunk[I2] => F2[O2])
    : Stream[F2, I] = stream.through(pipe.evalTapChunksInitLast(init)(last))

    def evalTapLastOpt[F2[x] >: F[x], I2 >: I, O2](f: Option[I2] => F2[O2]): Stream[F2, I] =
      stream.through(pipe.evalTapLastOpt(f))

    def evalTapLast[F2[x] >: F[x] : Applicative, I2 >: I, O2](f: I2 => F2[O2]): Stream[F2, I] =
      stream.through(pipe.evalTapLast(f))

    def resource[R](f: I => Resource[F, R])(using F: Concurrent[F]): Resource[F, Ref[F, R]] =
      Resource.make(stream.uncons1.flatMap {
        case Some((head, tail)) =>
          for
            (resource, release) <- f(head).allocated
            ref <- Ref.of[F, R](resource)
            // 外部将资源关闭时会将releaseCell置为None
            releaseCell <- AtomicCell[F].of[Option[F[Unit]]](release.some)
            fiber <- tail
              .evalMap(a => releaseCell.evalModify {
                case Some(release) => release *> f(a).allocated.map((nextResource, nextRelease) => (nextRelease.some, nextResource.some))
                case finalized => (finalized, none[R]).pure[F]
              }.flatMap(_.fold(().pure[F])(ref.set)))
              .compile.drain.start
          yield
            (ref, releaseCell, fiber)
        case _ => NoSuchElementException("empty stream")
          .raiseError[F, (Ref[F, R], AtomicCell[F, Option[F[Unit]]], Fiber[F, Throwable, Unit])]
      }) {
        case (ref, releaseCell, fiber) => releaseCell.evalUpdate {
          case Some(release) => release.as(none[F[Unit]])
          case finalized => finalized.pure[F]
        } *> fiber.cancel
      }.map(_._1)
  end extension
end StreamSyntax
object StreamSyntax extends StreamSyntax
