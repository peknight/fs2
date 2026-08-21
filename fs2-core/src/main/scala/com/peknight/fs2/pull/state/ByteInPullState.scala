package com.peknight.fs2.pull.state

import cats.data.StateT
import com.peknight.cats.instances.eitherT.given
import com.peknight.fs2.pull.state.PullState.{attempt as pullStateAttempt, output as pullStateOutput, outputE as pullStateOutputE, outputL as pullStateOutputL}
import fs2.Stream.ToPull
import fs2.{Chunk, Pipe, Pull, Stream}

import java.nio.charset.Charset
import scala.reflect.ClassTag

object ByteInPullState:
  def apply[F[_], O, S, E, A](f: (S, Stream[F, Byte]) => Pull[F, O, Either[E, ((S, Stream[F, Byte]), A)]])
  : ByteInPullState[F, O, S, E, A] =
    PullState[F, Byte, O, S, E, A](f)

  def pure[F[_], O, S, E, A](a: A): ByteInPullState[F, O, S, E, A] = PullState.pure[F, Byte, O, S, E, A](a)

  def unit[F[_], O, S, E]: ByteInPullState[F, O, S, E, Unit] = PullState.unit[F, Byte, O, S, E]

  def get[F[_], O, S, E]: ByteInPullState[F, O, S, E, (S, Stream[F, Byte])] = PullState.get[F, Byte, O, S, E]

  def getS[F[_], O, S, E]: ByteInPullState[F, O, S, E, S] = PullState.getS[F, Byte, O, S, E]

  def setS[F[_], O, S, E](s: S): ByteInPullState[F, O, S, E, Unit] = PullState.setS[F, Byte, O, S, E](s)

  def liftPE[F[_], O, S, E, A](pull: Pull[F, O, Either[E, A]]): ByteInPullState[F, O, S, E, A] =
    PullState.liftPE[F, Byte, O, S, E, A](pull)
  def liftP[F[_], O, S, E, A](pull: Pull[F, O, A]): ByteInPullState[F, O, S, E, A] =
    PullState.liftP[F, Byte, O, S, E, A](pull)
  def liftPL[F[_], O, S, E, A](pull: Pull[F, O, E]): ByteInPullState[F, O, S, E, A] =
    PullState.liftPL[F, Byte, O, S, E, A](pull)

  def liftFE[F[_], O, S, E, A](f: F[Either[E, A]]): ByteInPullState[F, O, S, E, A] =
    PullState.liftFE[F, Byte, O, S, E, A](f)
  def liftF[F[_], O, S, E, A](f: F[A]): ByteInPullState[F, O, S, E, A] =
    PullState.liftF[F, Byte, O, S, E, A](f)
  def liftFL[F[_], O, S, E, A](f: F[E]): ByteInPullState[F, O, S, E, A] =
    PullState.liftFL[F, Byte, O, S, E, A](f)

  def liftE[F[_], O, S, E, A](either: Either[E, A]): ByteInPullState[F, O, S, E, A] =
    PullState.liftE[F, Byte, O, S, E, A](either)
  def liftL[F[_], O, S, E, A](e: E): ByteInPullState[F, O, S, E, A] = PullState.liftL[F, Byte, O, S, E, A](e)

  def liftPET[F[_], O, S, E, A](pull: Pull[F, O, Either[Throwable, A]])(error: (S, Throwable) => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.liftPET[F, Byte, O, S, E, A](pull)(error)
  def liftPLT[F[_], O, S, E, A](pull: Pull[F, O, Throwable])(error: (S, Throwable) => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.liftPLT[F, Byte, O, S, E, A](pull)(error)
  def liftFET[F[_], O, S, E, A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.liftFET[F, Byte, O, S, E, A](f)(error)
  def liftFLT[F[_], O, S, E, A](f: F[Throwable])(error: (S, Throwable) => E): ByteInPullState[F, O, S, E, A] =
    PullState.liftFLT[F, Byte, O, S, E, A](f)(error)
  def liftET[F[_], O, S, E, A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.liftET[F, Byte, O, S, E, A](either)(error)
  def liftT[F[_], O, S, E, A](t: Throwable)(error: (S, Throwable) => E): ByteInPullState[F, O, S, E, A] =
    PullState.liftT[F, Byte, O, S, E, A](t)(error)

  def output[F[_], O, S, E](chunk: Chunk[O]): ByteInPullState[F, O, S, E, Unit] =
    PullState.output[F, Byte, O, S, E](chunk)

  def output[F[_], O, S, E](os: O*): ByteInPullState[F, O, S, E, Unit] = PullState.output[F, Byte, O, S, E](os*)

  def output1[F[_], O, S, E](o: O): ByteInPullState[F, O, S, E, Unit] = PullState.output1[F, Byte, O, S, E](o)

  def pipe[F[_], O, S, E](pipe: Pipe[F, Byte, O]): ByteInPullState[F, O, S, E, Unit] =
    PullState.pipe[F, Byte, O, S, E](pipe)

  def typed[F[_], O, S, E, A, B: ClassTag](a: A)(f: (S, A) => E): ByteInPullState[F, O, S, E, B] =
    PullState.typed[F, Byte, O, S, E, A, B](a)(f)

  def typedS[F[_], O, S, E, A: ClassTag](f: S => E): ByteInPullState[F, O, S, E, A] =
    PullState.typedS[F, Byte, O, S, E, A](f)

  def pull[F[_], O, S, E, A](f: ToPull[F, Byte] => Pull[F, O, Option[(A, Stream[F, Byte])]])
                            (eof: S => E): ByteInPullState[F, O, S, E, A] =
    PullState.pull[F, Byte, O, S, E, A](f)(eof)

  def map[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                              (g: I => A)(eof: S => E): ByteInPullState[F, O, S, E, A] =
    PullState.map[F, Byte, I, O, S, E, A](f)(g)(eof)

  def map1[F[_], O, S, E, A](f: Byte => A)(eof: S => E): ByteInPullState[F, O, S, E, A] =
    PullState.map1[F, Byte, O, S, E, A](f)(eof)

  def mapChunk[F[_], O, S, E, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                (g: Chunk[Byte] => A)(eof: S => E): ByteInPullState[F, O, S, E, A] =
    PullState.mapChunk[F, Byte, O, S, E, A](f)(g)(eof)

  def parse[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, O, Option[(I, Stream[F, Byte])]])
                                (g: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.parse[F, Byte, I, O, S, E, A](f)(g)(error)(eof)

  def parse1[F[_], O, S, E, A](f: Byte => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.parse1[F, Byte, O, S, E, A](f)(error)(eof)

  def parseChunk[F[_], O, S, E, A](f: ToPull[F, Byte] => Pull[F, O, Option[(Chunk[Byte], Stream[F, Byte])]])
                                  (g: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : ByteInPullState[F, O, S, E, A] =
    PullState.parseChunk[F, Byte, O, S, E, A](f)(g)(error)(eof)

  def readSizedBytes[F[_], O, S, E](eof: S => E): ByteInPullState[F, O, S, E, Chunk[Byte]] =
    for
      n <- pull[F, O, S, E, Byte](_.uncons1)(eof)
      chunk <- pull[F, O, S, E, Chunk[Byte]](_.unconsN(n))(eof)
    yield
      chunk

  def mapSizedBytes[F[_], O, S, E, A](f: Chunk[Byte] => A)(eof: S => E)
  : ByteInPullState[F, O, S, E, A] =
    readSizedBytes[F, O, S, E](eof).map(f)

  def parseSizedBytes[F[_], O, S, E, A](f: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)
                                       (eof: S => E): ByteInPullState[F, O, S, E, A] =
    for
      chunk <- readSizedBytes[F, O, S, E](eof)
      value <- liftET[F, O, S, E, A](f(chunk))(error)
    yield
      value

  def readSizedString[F[_], O, S, E](error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : ByteInPullState[F, O, S, E, String] =
    parseSizedBytes[F, O, S, E, String](_.toByteVector.decodeString)(error)(eof)

  def mapSizedString[F[_], O, S, E, A](f: String => A)(error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : ByteInPullState[F, O, S, E, A] =
    readSizedString[F, O, S, E](error)(eof).map(f)

  def parseSizedString[F[_], O, S, E, A](f: String => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
                                        (using Charset): ByteInPullState[F, O, S, E, A] =
    for
      value <- readSizedString[F, O, S, E](error)(eof)
      value <- liftET[F, O, S, E, A](f(value))(error)
    yield
      value

  extension [F[_], O, S, E, A] (state: ByteInPullState[F, O, S, E, A])
    def attempt(error: (S, Throwable) => E): ByteInPullState[F, O, S, E, A] = state.pullStateAttempt(error)
    def outputE(f: Either[E, A] => Chunk[O]): ByteInPullState[F, O, S, E, A] =
      state.pullStateOutputE(f)
    def output(f: A => Chunk[O]): ByteInPullState[F, O, S, E, A] =
      state.pullStateOutput(f)
    def outputL(f: E => Chunk[O]): ByteInPullState[F, O, S, E, A] =
      state.pullStateOutputL(f)
  end extension
end ByteInPullState
