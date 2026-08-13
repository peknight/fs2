package com.peknight.fs2.pull.state

import cats.data.StateT
import com.peknight.fs2.pull.state.PullState.attempt as pullStateAttempt
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, RaiseThrowable, Stream}

import java.io.EOFException
import java.nio.charset.Charset

object BytePullState:
  def apply[F[_], O, A](f: Stream[F, Byte] => Pull[F, O, (Stream[F, Byte], A)]): BytePullState[F, O, A] = StateT(f)

  def pure[F[_], O, A](a: A): BytePullState[F, O, A] = StateT.pure(a)

  def unit[F[_], O]: BytePullState[F, O, Unit] = pure[F, O, Unit](())

  def liftP[F[_], O, A](f: Pull[F, O, A]): BytePullState[F, O, A] = StateT.liftF(f)

  def liftF[F[_], O, A](f: F[A]): BytePullState[F, O, A] = liftP[F, O, A](Pull.eval(f))

  def raiseError[F[_] : RaiseThrowable, O, A](e: Throwable): BytePullState[F, O, A] = liftP(Pull.raiseError(e))

  def liftE[F[_]: RaiseThrowable, O, A](either: Either[Throwable, A]): BytePullState[F, O, A] =
    PullState.liftE[F, Byte, O, A](either)

  def output[F[_], O](chunk: Chunk[O]): BytePullState[F, O, Unit] = PullState.output[F, Byte, O](chunk)

  def output[F[_], O](os: O*): BytePullState[F, O, Unit] = PullState.output[F, Byte, O](os*)

  def output1[F[_], O](o: O): BytePullState[F, O, Unit] = PullState.output1[F, Byte, O](o)

  def pull[F[_]: RaiseThrowable, O, A](f: ToPull[F, Byte] => Pull[F, O, Option[(A, Stream[F, Byte])]])(eof: => Throwable)
  : BytePullState[F, O, A] =
    PullState.pull[F, Byte, O, A](f)(eof)

  def map[F[_]: RaiseThrowable, I, O, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                                        (g: I => A)(eof: => Throwable = new EOFException()): BytePullState[F, O, A] =
    PullState.map[F, Byte, I, O, A](f)(g)(eof)

  def map1[F[_]: RaiseThrowable, O, A](f: Byte => A)(eof: => Throwable = new EOFException()): BytePullState[F, O, A] =
    PullState.map1[F, Byte, O, A](f)(eof)

  def mapChunk[F[_]: RaiseThrowable, O, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                          (g: Chunk[Byte] => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, A] =
    PullState.mapChunk[F, Byte, O, A](f)(g)(eof)

  def parse[F[_]: RaiseThrowable, I, O, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                                          (g: I => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : BytePullState[F, O, A] =
    PullState.parse[F, Byte, I, O, A](f)(g)(eof)

  def parse1[F[_]: RaiseThrowable, O, A](f: Byte => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : BytePullState[F, O, A] =
    PullState.parse1[F, Byte, O, A](f)(eof)

  def parseChunk[F[_]: RaiseThrowable, O, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                            (g: Chunk[Byte] => Either[Throwable, A])
                                            (eof: => Throwable = new EOFException()): BytePullState[F, O, A] =
    PullState.parseChunk[F, Byte, O, A](f)(g)(eof)

  def readSizedBytes[F[_]: RaiseThrowable, O](eof: => Throwable = new EOFException()): BytePullState[F, O, Chunk[Byte]] =
    for
      n <- pull[F, O, Byte](_.uncons1)(eof)
      chunk <- pull[F, O, Chunk[Byte]](_.unconsN(n))(eof)
    yield
      chunk

  def mapSizedBytes[F[_]: RaiseThrowable, O, A](f: Chunk[Byte] => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, A] =
    readSizedBytes[F, O](eof).map(f)

  def parseSizedBytes[F[_]: RaiseThrowable, O, A](f: Chunk[Byte] => Either[Throwable, A])
                                                 (eof: => Throwable = new EOFException()): BytePullState[F, O, A] =
    for
      chunk <- readSizedBytes[F, O](eof)
      value <- liftE[F, O, A](f(chunk))
    yield
      value

  def readSizedString[F[_], O](eof: => Throwable = new EOFException())(using Charset)(using RaiseThrowable[F])
  : BytePullState[F, O, String] =
    parseSizedBytes[F, O, String](_.toByteVector.decodeString)(eof)

  def mapSizedString[F[_], O, A](f: String => A)(eof: => Throwable = new EOFException())(using Charset)
                                (using RaiseThrowable[F]): BytePullState[F, O, A] =
    readSizedString[F, O](eof).map(f)

  def parseSizedString[F[_], O, A](f: String => Either[Throwable, A])(eof: => Throwable = new EOFException())
                                  (using Charset)(using RaiseThrowable[F]): BytePullState[F, O, A] =
    for
      value <- readSizedString[F, O](eof)
      value <- liftE[F, O, A](f(value))
    yield
      value

  extension [F[_], O, A] (state: BytePullState[F, O, A])
    def attempt: BytePullState[F, O, Either[Throwable, A]] = state.pullStateAttempt
  end extension
end BytePullState
