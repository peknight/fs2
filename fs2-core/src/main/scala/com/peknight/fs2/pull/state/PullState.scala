package com.peknight.fs2.pull.state

import cats.data.StateT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.peknight.cats.instances.eitherT.given
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, Stream}

object PullState:
  def apply[F[_], I, O, S, E, A](f: (S, Stream[F, I]) => Pull[F, O, Either[E, ((S, Stream[F, I]), A)]])
  : PullState[F, I, O, S, E, A] =
    StateT(f.tupled)

  def pure[F[_], I, O, S, E, A](a: A): PullState[F, I, O, S, E, A] = StateT.pure(a)

  def unit[F[_], I, O, S, E]: PullState[F, I, O, S, E, Unit] = pure[F, I, O, S, E, Unit](())

  def get[F[_], I, O, S, E]: PullState[F, I, O, S, E, S] =
    StateT.get[[X] =>> Pull[F, O, Either[E, X]], (S, Stream[F, I])].map(_._1)

  def liftPE[F[_], I, O, S, E, A](pull: Pull[F, O, Either[E, A]]): PullState[F, I, O, S, E, A] = StateT.liftF(pull)
  def liftP[F[_], I, O, S, E, A](pull: Pull[F, O, A]): PullState[F, I, O, S, E, A] = liftPE(pull.map(_.asRight[E]))
  def liftPL[F[_], I, O, S, E, A](pull: Pull[F, O, E]): PullState[F, I, O, S, E, A] = liftPE(pull.map(_.asLeft[A]))

  def liftFE[F[_], I, O, S, E, A](f: F[Either[E, A]]): PullState[F, I, O, S, E, A] =
    liftPE[F, I, O, S, E, A](Pull.eval(f))
  def liftF[F[_], I, O, S, E, A](f: F[A]): PullState[F, I, O, S, E, A] =
    liftP[F, I, O, S, E, A](Pull.eval(f))
  def liftFL[F[_], I, O, S, E, A](f: F[E]): PullState[F, I, O, S, E, A] =
    liftPL[F, I, O, S, E, A](Pull.eval(f))

  def liftE[F[_], I, O, S, E, A](either: Either[E, A]): PullState[F, I, O, S, E, A] =
    either match
      case Right(value) => pure[F, I, O, S, E, A](value)
      case Left(error) => liftL[F, I, O, S, E, A](error)
  def liftL[F[_], I, O, S, E, A](e: E): PullState[F, I, O, S, E, A] = liftPL(Pull.pure(e))

  def liftPET[F[_], I, O, S, E, A](pull: Pull[F, O, Either[Throwable, A]])(error: (S, Throwable) => E)
  : PullState[F, I, O, S, E, A] =
    liftP[F, I, O, S, E, Either[Throwable, A]](pull).flatMap {
      case Right(a) => pure(a)
      case Left(e) => get[F, I, O, S, E].flatMap(state => liftL(error(state, e)))
    }
  def liftPLT[F[_], I, O, S, E, A](pull: Pull[F, O, Throwable])(error: (S, Throwable) => E)
  : PullState[F, I, O, S, E, A] =
    liftPET(pull.map(_.asLeft[A]))(error)
  def liftFET[F[_], I, O, S, E, A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : PullState[F, I, O, S, E, A] =
    liftPET[F, I, O, S, E, A](Pull.eval(f))(error)
  def liftFLT[F[_], I, O, S, E, A](f: F[Throwable])(error: (S, Throwable) => E): PullState[F, I, O, S, E, A] =
    liftPLT[F, I, O, S, E, A](Pull.eval(f))(error)
  def liftET[F[_], I, O, S, E, A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : PullState[F, I, O, S, E, A] =
    liftPET(Pull.pure(either))(error)
  def liftT[F[_], I, O, S, E, A](t: Throwable)(error: (S, Throwable) => E): PullState[F, I, O, S, E, A] =
    liftPLT(Pull.pure(t))(error)

  def output[F[_], I, O, S, E](chunk: Chunk[O]): PullState[F, I, O, S, E, Unit] =
    liftP[F, I, O, S, E, Unit](Pull.output[F, O](chunk))

  def output[F[_], I, O, S, E](os: O*): PullState[F, I, O, S, E, Unit] =
    liftP[F, I, O, S, E, Unit](Pull.output[F, O](Chunk(os*)))

  def output1[F[_], I, O, S, E](o: O): PullState[F, I, O, S, E, Unit] =
    liftP[F, I, O, S, E, Unit](Pull.output1[F, O](o))

  def pull[F[_], I, O, S, E, A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])(eof: S => E)
  : PullState[F, I, O, S, E, A] =
    apply[F, I, O, S, E, A]((s, stream) => f(stream.pull).flatMap {
      case Some((a, tail)) => Pull.pure(((s, tail), a).asRight)
      case _ => Pull.pure(eof(s).asLeft)
    })

  def map[F[_], I1, I2, O, S, E, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])
                                   (g: I2 => A)(eof: S => E): PullState[F, I1, O, S, E, A] =
    pull[F, I1, O, S, E, I2](f)(eof).map(g)

  def map1[F[_], I, O, S, E, A](f: I => A)(eof: S => E): PullState[F, I, O, S, E, A] =
    map[F, I, I, O, S, E, A](_.uncons1)(f)(eof)

  def mapChunk[F[_], I, O, S, E, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                   (g: Chunk[I] => A)(eof: S => E): PullState[F, I, O, S, E, A] =
    map[F, I, Chunk[I], O, S, E, A](f)(g)(eof)

  def parse[F[_], I1, I2, O, S, E, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])
                                     (g: I2 => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PullState[F, I1, O, S, E, A] =
    for
      i <- pull[F, I1, O, S, E, I2](f)(eof)
      a <- liftET[F, I1, O, S, E, A](g(i))(error)
    yield
      a

  def parse1[F[_], I, O, S, E, A](f: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PullState[F, I, O, S, E, A] =
    parse[F, I, I, O, S, E, A](_.uncons1)(f)(error)(eof)

  def parseChunk[F[_], I, O, S, E, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                     (g: Chunk[I] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PullState[F, I, O, S, E, A] =
    parse[F, I, Chunk[I], O, S, E, A](f)(g)(error)(eof)

  extension [F[_], I, O, S, E, A] (state: PullState[F, I, O, S, E, A])
    def attempt(error: (S, Throwable) => E): PullState[F, I, O, S, E, A] =
      apply((s, stream) => state.run((s, stream)).attempt.map {
        case Right(Right(tuple)) => tuple.asRight[E]
        case Right(Left(e)) => e.asLeft[((S, Stream[F, I]), A)]
        case Left(t) => error(s, t).asLeft[((S, Stream[F, I]), A)]
      })
    def outputE(f: Either[E, A] => Chunk[O]): PullState[F, I, O, S, E, A] =
      apply((s, stream) => state.run((s, stream)).flatMap {
        case Right(((s, stream), value)) => Pull.output(f(value.asRight[E])).as(((s, stream), value).asRight[E])
        case Left(e) => Pull.output(f(e.asLeft[A])).as(e.asLeft[((S, Stream[F, I]), A)])
      })
    def output(f: A => Chunk[O]): PullState[F, I, O, S, E, A] =
      state.flatMap(a => PullState.output(f(a)).as(a))
    def outputL(f: E => Chunk[O]): PullState[F, I, O, S, E, A] = {
      outputE {
        case Right(a) => Chunk.empty
        case Left(error) => f(error)
      }
    }
  end extension
end PullState
