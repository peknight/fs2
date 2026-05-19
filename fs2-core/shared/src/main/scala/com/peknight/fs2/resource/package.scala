package com.peknight.fs2

import _root_.fs2.Stream
import cats.syntax.option.*
import cats.effect.*
import cats.effect.std.AtomicCell
import cats.effect.syntax.spawn.*
import cats.syntax.applicative.*
import cats.syntax.applicativeError.*
import cats.syntax.apply.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import com.peknight.fs2.syntax.stream.uncons1

import java.util.NoSuchElementException

package object resource:
  def latest[F[_], I, O](stream: Stream[F, I])(f: I => Resource[F, O])(using F: Concurrent[F])
  : Resource[F, Ref[F, O]] =
    Resource.make(stream.uncons1.flatMap {
      case Some((head, tail)) =>
        for
          (resource, release) <- f(head).allocated
          ref <- Ref.of[F, O](resource)
          // 外部将资源关闭时会将releaseCell置为None
          releaseCell <- AtomicCell[F].of[Option[F[Unit]]](release.some)
          fiber <- tail
            .evalMap(a => releaseCell.evalModify {
              case Some(release) => release *> f(a).allocated.map((nextResource, nextRelease) => (nextRelease.some, nextResource.some))
              case finalized => (finalized, none[O]).pure[F]
            }.flatMap(_.fold(().pure[F])(ref.set)))
            .compile.drain.start
        yield
          (ref, releaseCell, fiber)
      case _ => NoSuchElementException("empty stream")
        .raiseError[F, (Ref[F, O], AtomicCell[F, Option[F[Unit]]], Fiber[F, Throwable, Unit])]
    }) {
      case (ref, releaseCell, fiber) => releaseCell.evalUpdate {
        case Some(release) => release.as(none[F[Unit]])
        case finalized => finalized.pure[F]
      } *> fiber.cancel
    }.map(_._1)
end resource