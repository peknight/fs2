package com.peknight.fs2

import _root_.fs2.Stream
import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Deferred, Fiber, Resource, Ref}
import cats.syntax.apply.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.option.*

package object resource:

  /**
   * 将流转化为 Resource，每当流产出新元素时：
   * 1. 释放前一个元素对应的 Resource
   * 2. 用 f 将新元素转化为 Resource 并获取
   * 3. 将最新值存入 Ref
   * 整体资源释放时，最后一个持有者会被安全释放。
   * 如果流未产出任何元素，将 raiseError。
   */
  def latest[F[_], A, B](stream: Stream[F, A])(f: A => Resource[F, B])(using F: Concurrent[F])
  : Resource[F, Ref[F, B]] =

    type State = Option[(B, F[Unit])]

    def processOne(state: Ref[F, State], ready: Deferred[F, Unit])(a: A): F[Unit] =
      for
        (oldRelease, isFirst) <- state.modify {
          case Some((_, rel)) => (None, (rel, false))
          case None           => (None, (F.unit, true))
        }
        _ <- oldRelease
        _ <- f(a).allocated.flatMap { case (b, release) =>
          state.set(Some((b, release)))
        }
        _ <- if (isFirst) ready.complete(()).void else F.unit
      yield ()

    def acquire: F[(Ref[F, State], Fiber[F, Throwable, Unit])] =
      Ref[F].of(none[(B, F[Unit])]).flatMap { state =>
        Deferred[F, Unit].flatMap { ready =>
          val fiber: F[Fiber[F, Throwable, Unit]] = stream.evalMap(processOne(state, ready)).compile.drain.start
          fiber.flatMap { f =>
            ready.get.as((state, f))
          }
        }
      }

    def release(tuple: (Ref[F, State], Fiber[F, Throwable, Unit])): F[Unit] =
      val (state, fiber) = tuple
      state.getAndSet(None).flatMap {
        case Some((_, release)) => release
        case None               => F.unit
      } *> fiber.cancel

    Resource.make(acquire)(release).map { case (state, _) =>
      state.asInstanceOf[Ref[F, B]]
    }

end resource