package com.peknight.fs2.pull.state

import cats.data.StateT
import com.peknight.cats.instances.eitherT.given
import com.peknight.fs2.pull.state.PullState.{attempt as pullStateAttempt, output as pullStateOutput, outputE as pullStateOutputE, outputL as pullStateOutputL}
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, Stream}
import scodec.bits.ByteVector

import java.nio.charset.Charset

object BytePullState:
  def apply[F[_], S, E, A](f: (S, Stream[F, Byte]) => Pull[F, Byte, Either[E, ((S, Stream[F, Byte]), A)]])
  : BytePullState[F, S, E, A] =
    StateT(f.tupled)

  def pure[F[_], S, E, A](a: A): BytePullState[F, S, E, A] = StateT.pure(a)

  def unit[F[_], S, E]: BytePullState[F, S, E, Unit] = pure[F, S, E, Unit](())

  def get[F[_], S, E]: BytePullState[F, S, E, S] = PullState.get[F, Byte, O, S, E]

  def liftPE[F[_], S, E, A](pull: Pull[F, Either[E, A]]): BytePullState[F, S, E, A] =
    PullState.liftPE[F, Byte, O, S, E, A](pull)
  def liftP[F[_], S, E, A](pull: Pull[F, A]): BytePullState[F, S, E, A] =
    PullState.liftP[F, Byte, O, S, E, A](pull)
  def liftPL[F[_], S, E, A](pull: Pull[F, E]): BytePullState[F, S, E, A] =
    PullState.liftPL[F, Byte, O, S, E, A](pull)

  def liftFE[F[_], S, E, A](f: F[Either[E, A]]): BytePullState[F, S, E, A] =
    PullState.liftFE[F, Byte, O, S, E, A](f)
  def liftF[F[_], S, E, A](f: F[A]): BytePullState[F, S, E, A] =
    PullState.liftF[F, Byte, O, S, E, A](f)
  def liftFL[F[_], S, E, A](f: F[E]): BytePullState[F, S, E, A] =
    PullState.liftFL[F, Byte, O, S, E, A](f)

  def liftE[F[_], S, E, A](either: Either[E, A]): BytePullState[F, S, E, A] =
    PullState.liftE[F, Byte, O, S, E, A](either)
  def liftL[F[_], S, E, A](e: E): BytePullState[F, S, E, A] = PullState.liftL[F, Byte, O, S, E, A](e)

  def liftPET[F[_], S, E, A](pull: Pull[F, Either[Throwable, A]])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    PullState.liftPET[F, Byte, O, S, E, A](pull)(error)
  def liftPLT[F[_], S, E, A](pull: Pull[F, Throwable])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    PullState.liftPLT[F, Byte, O, S, E, A](pull)(error)
  def liftFET[F[_], S, E, A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    PullState.liftFET[F, Byte, O, S, E, A](f)(error)
  def liftFLT[F[_], S, E, A](f: F[Throwable])(error: (S, Throwable) => E): BytePullState[F, S, E, A] =
    PullState.liftFLT[F, Byte, O, S, E, A](f)(error)
  def liftET[F[_], S, E, A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    PullState.liftET[F, Byte, O, S, E, A](either)(error)
  def liftT[F[_], S, E, A](t: Throwable)(error: (S, Throwable) => E): BytePullState[F, S, E, A] =
    PullState.liftT[F, Byte, O, S, E, A](t)(error)

  def output[F[_], S, E](chunk: Chunk[O]): BytePullState[F, S, E, Unit] =
    PullState.output[F, Byte, O, S, E](chunk)

  def output[F[_], S, E](os: O*): BytePullState[F, S, E, Unit] = PullState.output[F, Byte, O, S, E](os*)

  def output[F[_], S, E](bytes: ByteVector): BytePullState[F, S, E, Unit] =
    ByteInPullState.output[F, Byte, S, E](Chunk.byteVector(bytes))

  def output1[F[_], S, E](o: O): BytePullState[F, S, E, Unit] = PullState.output1[F, Byte, O, S, E](o)

  def pull[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Option[(A, Stream[F, Byte])]])
                            (eof: S => E): BytePullState[F, S, E, A] =
    PullState.pull[F, Byte, O, S, E, A](f)(eof)

  def map[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, Option[(I, Stream[F, Byte])]])
                              (g: I => A)(eof: S => E): BytePullState[F, S, E, A] =
    PullState.map[F, Byte, I, O, S, E, A](f)(g)(eof)

  def map1[F[_], S, E, A](f: Byte => A)(eof: S => E): BytePullState[F, S, E, A] =
    PullState.map1[F, Byte, O, S, E, A](f)(eof)

  def mapChunk[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Option[(Chunk[Byte], Stream[F, Byte])]])
                                (g: Chunk[Byte] => A)(eof: S => E): BytePullState[F, S, E, A] =
    PullState.mapChunk[F, Byte, O, S, E, A](f)(g)(eof)

  def parse[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, Option[(I, Stream[F, Byte])]])
                                (g: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    PullState.parse[F, Byte, I, O, S, E, A](f)(g)(error)(eof)

  def parse1[F[_], S, E, A](f: Byte => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    PullState.parse1[F, Byte, O, S, E, A](f)(error)(eof)

  def parseChunk[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Option[(Chunk[Byte], Stream[F, Byte])]])
                                  (g: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    PullState.parseChunk[F, Byte, O, S, E, A](f)(g)(error)(eof)

  def readSizedBytes[F[_], S, E](eof: S => E): BytePullState[F, S, E, Chunk[Byte]] =
    for
      n <- pull[F, S, E, Byte](_.uncons1)(eof)
      chunk <- pull[F, S, E, Chunk[Byte]](_.unconsN(n))(eof)
    yield
      chunk

  def mapSizedBytes[F[_], S, E, A](f: Chunk[Byte] => A)(eof: S => E)
  : BytePullState[F, S, E, A] =
    readSizedBytes[F, S, E](eof).map(f)

  def parseSizedBytes[F[_], S, E, A](f: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)
                                       (eof: S => E): BytePullState[F, S, E, A] =
    for
      chunk <- readSizedBytes[F, S, E](eof)
      value <- liftET[F, S, E, A](f(chunk))(error)
    yield
      value

  def readSizedString[F[_], S, E](error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : BytePullState[F, S, E, String] =
    parseSizedBytes[F, S, E, String](_.toByteVector.decodeString)(error)(eof)

  def mapSizedString[F[_], S, E, A](f: String => A)(error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : BytePullState[F, S, E, A] =
    readSizedString[F, S, E](error)(eof).map(f)

  def parseSizedString[F[_], S, E, A](f: String => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
                                        (using Charset): BytePullState[F, S, E, A] =
    for
      value <- readSizedString[F, S, E](error)(eof)
      value <- liftET[F, S, E, A](f(value))(error)
    yield
      value

  extension [F[_], S, E, A] (state: BytePullState[F, S, E, A])
    def attempt(error: (S, Throwable) => E): BytePullState[F, S, E, A] = state.pullStateAttempt(error)
    def outputE(f: Either[E, A] => Chunk[O]): BytePullState[F, S, E, A] =
      state.pullStateOutputE(f)
    def output(f: A => Chunk[O]): BytePullState[F, S, E, A] =
      state.pullStateOutput(f)
    def outputL(f: E => Chunk[O]): BytePullState[F, S, E, A] =
      state.pullStateOutputL(f)
  end extension
end BytePullState
