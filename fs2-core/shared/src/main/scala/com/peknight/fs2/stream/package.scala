package com.peknight.fs2

import _root_.fs2.{Pull, Stream}
import cats.Monad
import cats.effect.kernel.Temporal
import cats.syntax.either.*
import cats.syntax.option.*

import scala.concurrent.duration.*

package object stream:
  def unfoldTemporal[F[_]: Temporal, A](f: Option[A] => F[(Option[A], Option[FiniteDuration])]): Stream[F, A] =
    def go: Pull[F, A, Unit] =
      Monad[[X] =>> Pull[F, A, X]].tailRecM[Option[A], Unit](None)(opt => Pull.eval(f(opt)).flatMap {
        case (Some(a), Some(duration)) => Pull.output1[F, A](a).flatMap(_ => Pull.sleep(duration)).as(a.some.asLeft)
        case (Some(a), _) => Pull.output1[F, A](a).as(().asRight[Option[A]])
        case (_, Some(duration)) => Pull.sleep(duration).as(opt.asLeft)
        case _ => Pull.pure(().asRight[Option[A]])
      })
    go.stream
end stream