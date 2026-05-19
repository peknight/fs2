package com.peknight.fs2

import _root_.fs2.{Pull, Stream}
import cats.Monad
import cats.effect.kernel.Temporal
import cats.syntax.either.*

import scala.concurrent.duration.*

package object stream:
  def unfoldTemporal[F[_]: Temporal, S, O](s: S)(f: S => F[(Option[(O, S)], Option[FiniteDuration])]): Stream[F, O] =
    def go: Pull[F, O, Unit] =
      Monad[[X] =>> Pull[F, O, X]].tailRecM[S, Unit](s)(s => Pull.eval(f(s)).flatMap {
        case (Some((o, nextS)), Some(duration)) => Pull.output1[F, O](o).flatMap(_ => Pull.sleep(duration)).as(nextS.asLeft)
        case (Some((o, _)), _) => Pull.output1[F, O](o).as(().asRight[S])
        case (_, Some(duration)) => Pull.sleep(duration).as(s.asLeft)
        case _ => Pull.pure(().asRight[S])
      })
    go.stream
end stream