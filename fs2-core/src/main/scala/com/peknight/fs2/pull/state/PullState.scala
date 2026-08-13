package com.peknight.fs2.pull.state

import cats.data.StateT
import cats.syntax.either.*
import cats.syntax.functor.*
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, RaiseThrowable, Stream}

import java.io.EOFException

object PullState:
  def apply[F[_], I, O, A](f: Stream[F, I] => Pull[F, O, (Stream[F, I], A)]): PullState[F, I, O, A] = StateT(f)

  def pure[F[_], I, O, A](a: A): PullState[F, I, O, A] = StateT.pure(a)

  def unit[F[_], I, O]: PullState[F, I, O, Unit] = pure[F, I, O, Unit](())

  def liftP[F[_], I, O, A](f: Pull[F, O, A]): PullState[F, I, O, A] = StateT.liftF(f)

  def liftF[F[_], I, O, A](f: F[A]): PullState[F, I, O, A] = liftP[F, I, O, A](Pull.eval(f))

  def raiseError[F[_] : RaiseThrowable, I, O, A](e: Throwable): PullState[F, I, O, A] = liftP(Pull.raiseError(e))

  def liftE[F[_]: RaiseThrowable, I, O, A](either: Either[Throwable, A]): PullState[F, I, O, A] =
    either match
      case Right(value) => pure[F, I, O, A](value)
      case Left(error) => raiseError[F, I, O, A](error)

  def output[F[_], I, O](chunk: Chunk[O]): PullState[F, I, O, Unit] = liftP[F, I, O, Unit](Pull.output[F, O](chunk))

  def output[F[_], I, O](os: O*): PullState[F, I, O, Unit] = liftP[F, I, O, Unit](Pull.output[F, O](Chunk(os*)))

  def output1[F[_], I, O](o: O): PullState[F, I, O, Unit] = liftP[F, I, O, Unit](Pull.output1[F, O](o))

  def pull[F[_]: RaiseThrowable, I, O, A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])
                                         (eof: => Throwable = new EOFException()): PullState[F, I, O, A] =
    apply[F, I, O, A](stream => f(stream.pull).flatMap {
      case Some(tuple) => Pull.pure(tuple.swap)
      case _ => Pull.raiseError(eof)
    })

  def map[F[_]: RaiseThrowable, I1, I2, O, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])(g: I2 => A)
                                             (eof: => Throwable = new EOFException()): PullState[F, I1, O, A] =
    pull[F, I1, O, I2](f)(eof).map(g)

  def map1[F[_]: RaiseThrowable, I, O, A](f: I => A)(eof: => Throwable = new EOFException()): PullState[F, I, O, A] =
    map[F, I, I, O, A](_.uncons1)(f)(eof)

  def mapChunk[F[_]: RaiseThrowable, I, O, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                             (g: Chunk[I] => A)(eof: => Throwable = new EOFException())
  : PullState[F, I, O, A] =
    map[F, I, Chunk[I], O, A](f)(g)(eof)

  def parse[F[_]: RaiseThrowable, I1, I2, O, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])
                                               (g: I2 => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : PullState[F, I1, O, A] =
    for
      i <- pull[F, I1, O, I2](f)(eof)
      a <- liftE[F, I1, O, A](g(i))
    yield
      a

  def parse1[F[_]: RaiseThrowable, I, O, A](f: I => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : PullState[F, I, O, A] =
    parse[F, I, I, O, A](_.uncons1)(f)(eof)

  def parseChunk[F[_]: RaiseThrowable, I, O, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                               (g: Chunk[I] => Either[Throwable, A])
                                               (eof: => Throwable = new EOFException()): PullState[F, I, O, A] =
    parse[F, I, Chunk[I], O, A](f)(g)(eof)

  extension [F[_], I, O, A] (state: PullState[F, I, O, A])
    def attempt: PullState[F, I, O, Either[Throwable, A]] =
      apply(stream => state.run(stream).attempt.flatMap {
        case Right((tail, value)) => Pull.pure((tail, value.asRight[Throwable]))
        case Left(error) => Pull.pure((stream, error.asLeft[A]))
      })
    def output(f: A => Chunk[O])(g: Throwable => Chunk[O])(using RaiseThrowable[F]): PullState[F, I, O, A] =
      outputE {
        case Right(a) => f(a)
        case Left(error) => g(error)
      }
    def outputE(f: Either[Throwable, A] => Chunk[O])(using RaiseThrowable[F]): PullState[F, I, O, A] =
      attempt.flatMap {
        case Right(a) =>
          PullState.output(f(a.asRight)).as(a)
        case Left(error) =>
          PullState.output(f(error.asLeft)).flatMap(_ => raiseError(error))
      }
  end extension
end PullState
