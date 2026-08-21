package com.peknight.fs2.pull.state

import com.peknight.fs2.pull.state.ByteInPullState.{attempt as pullStateAttempt, output as pullStateOutput, outputE as pullStateOutputE, outputL as pullStateOutputL}
import fs2.Stream.ToPull
import fs2.{Chunk, Pipe, Pull, Stream}
import scodec.bits.ByteVector

import java.nio.charset.Charset
import scala.reflect.ClassTag

object BytePullState:
  def apply[F[_], S, E, A](f: (S, Stream[F, Byte]) => Pull[F, Byte, Either[E, ((S, Stream[F, Byte]), A)]])
  : BytePullState[F, S, E, A] =
    ByteInPullState[F, Byte, S, E, A](f)

  def pure[F[_], S, E, A](a: A): BytePullState[F, S, E, A] = ByteInPullState.pure[F, Byte, S, E, A](a)

  def unit[F[_], S, E]: BytePullState[F, S, E, Unit] = ByteInPullState.unit[F, Byte, S, E]

  def get[F[_], S, E]: BytePullState[F, S, E, (S, Stream[F, Byte])] = ByteInPullState.get[F, Byte, S, E]

  def getS[F[_], S, E]: BytePullState[F, S, E, S] = ByteInPullState.getS[F, Byte, S, E]

  def setS[F[_], S, E](s: S): BytePullState[F, S, E, Unit] = ByteInPullState.setS[F, Byte, S, E](s)

  def liftPE[F[_], S, E, A](pull: Pull[F, Byte, Either[E, A]]): BytePullState[F, S, E, A] =
    ByteInPullState.liftPE[F, Byte, S, E, A](pull)
  def liftP[F[_], S, E, A](pull: Pull[F, Byte, A]): BytePullState[F, S, E, A] =
    ByteInPullState.liftP[F, Byte, S, E, A](pull)
  def liftPL[F[_], S, E, A](pull: Pull[F, Byte, E]): BytePullState[F, S, E, A] =
    ByteInPullState.liftPL[F, Byte, S, E, A](pull)

  def liftFE[F[_], S, E, A](f: F[Either[E, A]]): BytePullState[F, S, E, A] =
    ByteInPullState.liftFE[F, Byte, S, E, A](f)
  def liftF[F[_], S, E, A](f: F[A]): BytePullState[F, S, E, A] =
    ByteInPullState.liftF[F, Byte, S, E, A](f)
  def liftFL[F[_], S, E, A](f: F[E]): BytePullState[F, S, E, A] =
    ByteInPullState.liftFL[F, Byte, S, E, A](f)

  def liftE[F[_], S, E, A](either: Either[E, A]): BytePullState[F, S, E, A] =
    ByteInPullState.liftE[F, Byte, S, E, A](either)
  def liftL[F[_], S, E, A](e: E): BytePullState[F, S, E, A] = ByteInPullState.liftL[F, Byte, S, E, A](e)

  def liftPET[F[_], S, E, A](pull: Pull[F, Byte, Either[Throwable, A]])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.liftPET[F, Byte, S, E, A](pull)(error)
  def liftPLT[F[_], S, E, A](pull: Pull[F, Byte, Throwable])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.liftPLT[F, Byte, S, E, A](pull)(error)
  def liftFET[F[_], S, E, A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.liftFET[F, Byte, S, E, A](f)(error)
  def liftFLT[F[_], S, E, A](f: F[Throwable])(error: (S, Throwable) => E): BytePullState[F, S, E, A] =
    ByteInPullState.liftFLT[F, Byte, S, E, A](f)(error)
  def liftET[F[_], S, E, A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.liftET[F, Byte, S, E, A](either)(error)
  def liftT[F[_], S, E, A](t: Throwable)(error: (S, Throwable) => E): BytePullState[F, S, E, A] =
    ByteInPullState.liftT[F, Byte, S, E, A](t)(error)

  def output[F[_], S, E](chunk: Chunk[Byte]): BytePullState[F, S, E, Unit] =
    ByteInPullState.output[F, Byte, S, E](chunk)

  def output[F[_], S, E](os: Byte*): BytePullState[F, S, E, Unit] = ByteInPullState.output[F, Byte, S, E](os*)

  def output[F[_], S, E](bytes: ByteVector): BytePullState[F, S, E, Unit] =
    ByteInPullState.output[F, Byte, S, E](Chunk.byteVector(bytes))

  def output1[F[_], S, E](o: Byte): BytePullState[F, S, E, Unit] = ByteInPullState.output1[F, Byte, S, E](o)

  def pipe[F[_], S, E](pipe: Pipe[F, Byte, Byte]): BytePullState[F, S, E, Unit] =
    ByteInPullState.pipe[F, Byte, S, E](pipe)

  def typed[F[_], S, E, A, B: ClassTag](a: A)(f: (S, A) => E): BytePullState[F, S, E, B] =
    ByteInPullState.typed[F, Byte, S, E, A, B](a)(f)

  def typedS[F[_], S, E, A: ClassTag](f: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.typedS[F, Byte, S, E, A](f)

  def pull[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Byte, Option[(A, Stream[F, Byte])]])
                         (eof: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.pull[F, Byte, S, E, A](f)(eof)

  def map[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, Byte, Option[(I, Stream[F, Byte])]])
                              (g: I => A)(eof: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.map[F, I, Byte, S, E, A](f)(g)(eof)

  def map1[F[_], S, E, A](f: Byte => A)(eof: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.map1[F, Byte, S, E, A](f)(eof)

  def mapChunk[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Byte, Option[(Chunk[Byte], Stream[F, Byte])]])
                             (g: Chunk[Byte] => A)(eof: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.mapChunk[F, Byte, S, E, A](f)(g)(eof)

  def parse[F[_], I, O, S, E, A](f: ToPull[F, Byte] => Pull[F, Byte, Option[(I, Stream[F, Byte])]])
                                (g: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.parse[F, I, Byte, S, E, A](f)(g)(error)(eof)

  def parse1[F[_], S, E, A](f: Byte => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.parse1[F, Byte, S, E, A](f)(error)(eof)

  def parseChunk[F[_], S, E, A](f: ToPull[F, Byte] => Pull[F, Byte, Option[(Chunk[Byte], Stream[F, Byte])]])
                               (g: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.parseChunk[F, Byte, S, E, A](f)(g)(error)(eof)

  def readSizedBytes[F[_], S, E](eof: S => E): BytePullState[F, S, E, Chunk[Byte]] =
    ByteInPullState.readSizedBytes[F, Byte, S, E](eof)

  def mapSizedBytes[F[_], S, E, A](f: Chunk[Byte] => A)(eof: S => E)
  : BytePullState[F, S, E, A] =
    ByteInPullState.mapSizedBytes[F, Byte, S, E, A](f)(eof)

  def parseSizedBytes[F[_], S, E, A](f: Chunk[Byte] => Either[Throwable, A])(error: (S, Throwable) => E)
                                    (eof: S => E): BytePullState[F, S, E, A] =
    ByteInPullState.parseSizedBytes[F, Byte, S, E, A](f)(error)(eof)

  def readSizedString[F[_], S, E](error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : BytePullState[F, S, E, String] =
    ByteInPullState.readSizedString[F, Byte, S, E](error)(eof)

  def mapSizedString[F[_], S, E, A](f: String => A)(error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : BytePullState[F, S, E, A] =
    ByteInPullState.mapSizedString[F, Byte, S, E, A](f)(error)(eof)

  def parseSizedString[F[_], S, E, A](f: String => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
                                     (using Charset): BytePullState[F, S, E, A] =
    ByteInPullState.parseSizedString[F, Byte, S, E, A](f)(error)(eof)

  extension [F[_], S, E, A] (state: BytePullState[F, S, E, A])
    def attempt(error: (S, Throwable) => E): BytePullState[F, S, E, A] = state.pullStateAttempt(error)
    def outputE(f: Either[E, A] => ByteVector): BytePullState[F, S, E, A] =
      state.pullStateOutputE(f.andThen(Chunk.byteVector))
    def output(f: A => ByteVector): BytePullState[F, S, E, A] =
      state.pullStateOutput(f.andThen(Chunk.byteVector))
    def outputL(f: E => ByteVector): BytePullState[F, S, E, A] =
      state.pullStateOutputL(f.andThen(Chunk.byteVector))
  end extension
end BytePullState
