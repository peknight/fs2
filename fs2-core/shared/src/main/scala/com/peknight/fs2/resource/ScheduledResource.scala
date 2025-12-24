package com.peknight.fs2.resource

import cats.effect.std.AtomicCell
import cats.effect.syntax.spawn.*
import cats.effect.{Concurrent, MonadCancel, Ref, Resource}
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.option.*
import fs2.Stream

object ScheduledResource:

  private def init[F[_]: Concurrent, S, A](initF: F[S])(nextF: S => F[Option[S]])(resourceF: S => F[Resource[F, A]])
  : F[(AtomicCell[F, Option[F[Unit]]], S, A)] =
    for
      initS <- initF
      nextS <- nextF(initS).map(_.getOrElse(initS))
      resource <- resourceF(nextS)
      (initR, release) <- resource.allocated
      releaseCell <- AtomicCell[F].of[Option[F[Unit]]](release.some)
    yield
      (releaseCell, nextS, initR)

  private def next[F[_], S, A](releaseCell: AtomicCell[F, Option[F[Unit]]], ref: Ref[F, (S, A)], state: S)
                              (nextF: S => F[Option[S]])(resourceF: S => F[Resource[F, A]])
                              (using MonadCancel[F, Throwable]): F[Option[S]] =
    releaseCell.evalModify {
      case running @ Some(release) => nextF(state).flatMap {
        case Some(nextS) =>
          for
            _ <- release
            resource <- resourceF(nextS)
            (nextR, nextRelease) <- resource.allocated
          yield
            (nextRelease.some, (nextS.some, nextR.some))
        case _ => (running, (state.some, none[A])).pure[F]
      }
      case finalized => (finalized, (none[S], none[A])).pure[F]
    } flatMap {
      case (Some(nextS), Some(nextR)) => ref.set((nextS, nextR)).as(nextS.some)
      case (nextS, _) => nextS.pure[F]
    }

  def apply[F[_], S, A](scheduler: Stream[F, ?])(initF: F[S])(nextF: S => F[Option[S]])
                       (resourceF: S => F[Resource[F, A]])(using Concurrent[F])
  : Resource[F, Ref[F, (S, A)]] =
    Resource.make {
      for
        (releaseCell, initS, initR) <- init[F, S, A](initF)(nextF)(resourceF)
        ref <- Ref.of[F, (S, A)](initS, initR)
        fiber <- scheduler.zipRight(Stream.unfoldEval[F, S, Unit](initS)(state =>
          next[F, S, A](releaseCell, ref, state)(nextF)(resourceF).map(_.map(nextS => ((), nextS)))
        )).compile.drain.start
      yield
        (ref, releaseCell, fiber)
    } {
      case (ref, releaseCell, fiber) =>
        for
          _ <- releaseCell.evalUpdate {
            case Some(release) => release.as(none[F[Unit]])
            case finalized => finalized.pure[F]
          }
          _ <- fiber.cancel
        yield
          ()
    }.map(_._1)
end ScheduledResource