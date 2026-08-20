package com.peknight.fs2.pull.state

import cats.data.StateT
import com.peknight.fs2.pull.state.PullState.{attempt as pullStateAttempt, output as pullStateOutput, outputE as pullStateOutputE}
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, RaiseThrowable, Stream}
import scodec.bits.ByteVector

import java.io.EOFException
import java.nio.charset.Charset

object BytePullState:
  def apply[F[_], O, S, A](f: (S, Stream[F, Byte]) => Pull[F, O, ((S, Stream[F, Byte]), A)]): BytePullState[F, O, S, A] =
    StateT(f.tupled)

  def pure[F[_], O, S, A](a: A): BytePullState[F, O, S, A] = StateT.pure(a)

  def unit[F[_], O, S]: BytePullState[F, O, S, Unit] = pure[F, O, S, Unit](())

  def liftP[F[_], O, S, A](f: Pull[F, O, A]): BytePullState[F, O, S, A] = StateT.liftF(f)

  def liftF[F[_], O, S, A](f: F[A]): BytePullState[F, O, S, A] = liftP[F, O, S, A](Pull.eval(f))

  def raiseError[F[_] : RaiseThrowable, O, S, A](e: Throwable): BytePullState[F, O, S, A] = liftP(Pull.raiseError(e))

  def liftE[F[_]: RaiseThrowable, O, S, A](either: Either[Throwable, A]): BytePullState[F, O, S, A] =
    PullState.liftE[F, Byte, O, S, A](either)

  def output[F[_], O, S](chunk: Chunk[O]): BytePullState[F, O, S, Unit] = PullState.output[F, Byte, O, S](chunk)

  def output[F[_], O, S](os: O*): BytePullState[F, O, S, Unit] = PullState.output[F, Byte, O, S](os*)

  def output[F[_], S](bytes: ByteVector): BytePullState[F, Byte, S, Unit] =
    PullState.output[F, Byte, Byte, S](Chunk.byteVector(bytes))

  def output1[F[_], O, S](o: O): BytePullState[F, O, S, Unit] = PullState.output1[F, Byte, O, S](o)

  def pull[F[_]: RaiseThrowable, O, S, A](f: ToPull[F, Byte] => Pull[F, O, Option[(A, Stream[F, Byte])]])
                                         (eof: => Throwable = new EOFException()): BytePullState[F, O, S, A] =
    PullState.pull[F, Byte, O, S, A](f)(eof)

  def map[F[_]: RaiseThrowable, I, O, S, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                                           (g: I => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    PullState.map[F, Byte, I, O, S, A](f)(g)(eof)

  def map1[F[_]: RaiseThrowable, O, S, A](f: Byte => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    PullState.map1[F, Byte, O, S, A](f)(eof)

  def mapChunk[F[_]: RaiseThrowable, O, S, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                             (g: Chunk[Byte] => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    PullState.mapChunk[F, Byte, O, S, A](f)(g)(eof)

  def parse[F[_]: RaiseThrowable, I, O, S, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                                             (g: I => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    PullState.parse[F, Byte, I, O, S, A](f)(g)(eof)

  def parse1[F[_]: RaiseThrowable, O, S, A](f: Byte => Either[Throwable, A])(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    PullState.parse1[F, Byte, O, S, A](f)(eof)

  def parseChunk[F[_]: RaiseThrowable, O, S, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                               (g: Chunk[Byte] => Either[Throwable, A])
                                               (eof: => Throwable = new EOFException()): BytePullState[F, O, S, A] =
    PullState.parseChunk[F, Byte, O, S, A](f)(g)(eof)

  def readSizedBytes[F[_]: RaiseThrowable, O, S](eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, Chunk[Byte]] =
    for
      n <- pull[F, O, S, Byte](_.uncons1)(eof)
      chunk <- pull[F, O, S, Chunk[Byte]](_.unconsN(n))(eof)
    yield
      chunk

  def mapSizedBytes[F[_]: RaiseThrowable, O, S, A](f: Chunk[Byte] => A)(eof: => Throwable = new EOFException())
  : BytePullState[F, O, S, A] =
    readSizedBytes[F, O, S](eof).map(f)

  def parseSizedBytes[F[_]: RaiseThrowable, O, S, A](f: Chunk[Byte] => Either[Throwable, A])
                                                    (eof: => Throwable = new EOFException()): BytePullState[F, O, S, A] =
    for
      chunk <- readSizedBytes[F, O, S](eof)
      value <- liftE[F, O, S, A](f(chunk))
    yield
      value

  def readSizedString[F[_], O, S](eof: => Throwable = new EOFException())(using Charset)(using RaiseThrowable[F])
  : BytePullState[F, O, S, String] =
    parseSizedBytes[F, O, S, String](_.toByteVector.decodeString)(eof)

  def mapSizedString[F[_], O, S, A](f: String => A)(eof: => Throwable = new EOFException())(using Charset)
                                   (using RaiseThrowable[F]): BytePullState[F, O, S, A] =
    readSizedString[F, O, S](eof).map(f)

  def parseSizedString[F[_], O, S, A](f: String => Either[Throwable, A])(eof: => Throwable = new EOFException())
                                     (using Charset)(using RaiseThrowable[F]): BytePullState[F, O, S, A] =
    for
      value <- readSizedString[F, O, S](eof)
      value <- liftE[F, O, S, A](f(value))
    yield
      value

  extension [F[_], O, S, A] (state: BytePullState[F, O, S, A])
    def attempt: BytePullState[F, O, S, Either[Throwable, A]] = state.pullStateAttempt
    def output(f: A => Chunk[O])(g: Throwable => Chunk[O])(using RaiseThrowable[F]): BytePullState[F, O, S, A] =
      state.pullStateOutput(f)(g)
    def outputE(f: Either[Throwable, A] => Chunk[O])(using RaiseThrowable[F]): BytePullState[F, O, S, A] =
      state.pullStateOutputE(f)
  end extension
  extension [F[_], S, A] (state: BytePullState[F, Byte, S, A])
    def outputBytes(f: A => ByteVector)(g: Throwable => ByteVector)(using RaiseThrowable[F])
    : BytePullState[F, Byte, S, A] =
      state.pullStateOutput(a => Chunk.byteVector(f(a)))(e => Chunk.byteVector(g(e)))
    def outputBytesE(f: Either[Throwable, A] => ByteVector)(using RaiseThrowable[F]): BytePullState[F, Byte, S, A] =
      state.pullStateOutputE(either => Chunk.byteVector(f(either)))
  end extension
end BytePullState
