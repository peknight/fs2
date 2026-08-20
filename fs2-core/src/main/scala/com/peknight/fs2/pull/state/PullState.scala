package com.peknight.fs2.pull.state

import cats.data.StateT
import cats.syntax.either.*
import cats.syntax.functor.*
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, RaiseThrowable, Stream}

import java.io.EOFException

object PullState:
  def apply[F[_], I, O, S, A](f: (S, Stream[F, I]) => Pull[F, O, ((S, Stream[F, I]), A)]): PullState[F, I, O, S, A] =
    StateT(f.tupled)

  def pure[F[_], I, O, S, A](a: A): PullState[F, I, O, S, A] = StateT.pure(a)

  def unit[F[_], I, O, S]: PullState[F, I, O, S, Unit] = pure[F, I, O, S, Unit](())

  def liftP[F[_], I, O, S, A](f: Pull[F, O, A]): PullState[F, I, O, S, A] = StateT.liftF(f)

  def liftF[F[_], I, O, S, A](f: F[A]): PullState[F, I, O, S, A] = liftP[F, I, O, S, A](Pull.eval(f))

  def raiseError[F[_] : RaiseThrowable, I, O, S, A](e: Throwable): PullState[F, I, O, S, A] = liftP(Pull.raiseError(e))

  def liftE[F[_]: RaiseThrowable, I, O, S, A](either: Either[Throwable, A]): PullState[F, I, O, S, A] =
    either match
      case Right(value) => pure[F, I, O, S, A](value)
      case Left(error) => raiseError[F, I, O, S, A](error)

  def output[F[_], I, O, S](chunk: Chunk[O]): PullState[F, I, O, S, Unit] =
    liftP[F, I, O, S, Unit](Pull.output[F, O](chunk))

  def output[F[_], I, O, S](os: O*): PullState[F, I, O, S, Unit] =
    liftP[F, I, O, S, Unit](Pull.output[F, O](Chunk(os*)))

  def output1[F[_], I, O, S](o: O): PullState[F, I, O, S, Unit] =
    liftP[F, I, O, S, Unit](Pull.output1[F, O](o))

  def pull[F[_]: RaiseThrowable, I, O, S, A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])
                                            (eof: => Throwable = new EOFException()): PullState[F, I, O, S, A] =
    apply[F, I, O, S, A]((s, stream) => f(stream.pull).flatMap {
      case Some((a, tail)) => Pull.pure(((s, tail), a))
      case _ => Pull.raiseError(eof)
    })

  def map[F[_]: RaiseThrowable, I1, I2, O, S, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])
                                                (g: I2 => A)(eof: => Throwable = new EOFException())
  : PullState[F, I1, O, S, A] =
    pull[F, I1, O, S, I2](f)(eof).map(g)

  def map1[F[_]: RaiseThrowable, I, O, S, A](f: I => A)(eof: => Throwable = new EOFException())
  : PullState[F, I, O, S, A] =
    map[F, I, I, O, S, A](_.uncons1)(f)(eof)

  def mapChunk[F[_]: RaiseThrowable, I, O, S, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                                (g: Chunk[I] => A)(eof: => Throwable = new EOFException())
  : PullState[F, I, O, S, A] =
    map[F, I, Chunk[I], O, S, A](f)(g)(eof)

  def parse[F[_]: RaiseThrowable, I1, I2, O, S, A](f: ToPull[F, I1] => Pull[F, O, Option[(I2, Stream[F, I1])]])
                                                  (g: I2 => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : PullState[F, I1, O, S, A] =
    for
      i <- pull[F, I1, O, S, I2](f)(eof)
      a <- liftE[F, I1, O, S, A](g(i))
    yield
      a

  def parse1[F[_]: RaiseThrowable, I, O, S, A](f: I => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : PullState[F, I, O, S, A] =
    parse[F, I, I, O, S, A](_.uncons1)(f)(eof)

  def parseChunk[F[_]: RaiseThrowable, I, O, S, A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                                                  (g: Chunk[I] => Either[Throwable, A])
                                                  (eof: => Throwable = new EOFException()): PullState[F, I, O, S, A] =
    parse[F, I, Chunk[I], O, S, A](f)(g)(eof)

  extension [F[_], I, O, S, A] (state: PullState[F, I, O, S, A])
    def attempt: PullState[F, I, O, S, Either[Throwable, A]] =
      apply((s, stream) => state.run((s, stream)).attempt.flatMap {
        case Right((s, value)) => Pull.pure((s, value.asRight[Throwable]))
        case Left(error) => Pull.pure(((s, stream), error.asLeft[A]))
      })
    def output(f: A => Chunk[O])(g: Throwable => Chunk[O])(using RaiseThrowable[F]): PullState[F, I, O, S, A] =
      outputE {
        case Right(a) => f(a)
        case Left(error) => g(error)
      }
    def outputE(f: Either[Throwable, A] => Chunk[O])(using RaiseThrowable[F]): PullState[F, I, O, S, A] =
      attempt.flatMap {
        case Right(a) =>
          PullState.output(f(a.asRight)).as(a)
        case Left(error) =>
          PullState.output(f(error.asLeft)).flatMap(_ => raiseError(error))
      }
  end extension
end PullState
