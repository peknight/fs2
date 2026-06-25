package com.peknight.fs2.pull.state

import cats.data.IndexedStateT
import com.peknight.cats.instances.eitherT.eitherTMonad
import fs2.{Chunk, Pull, Stream}
import scodec.bits.ByteVector

import java.nio.charset.{CharacterCodingException, Charset}

object EitherPullState:
  def apply[F[_], O, E, SA, SB, I, A](f: ((SA, Stream[F, I])) => Pull[F, O, Either[E, ((SB, Stream[F, I]), A)]])
  : EitherPullState[F, O, E, SA, SB, I, A] =
    IndexedStateT(f)

  def unconsSizedString[F[_], O, E, S](eof: => E)(onError: (CharacterCodingException, ByteVector) => E)
                                      (using Charset): EitherPullState[F, O, E, S, S, Byte, String] =
    for
      chunk <- unconsSizedBytes[F, O, E, S](eof)
      bytes = chunk.toByteVector
      value <- liftEither[F, O, E, S, Byte, String](bytes.decodeString.left.map(e => onError(e, bytes)))
    yield
      value

  def unconsSizedBytes[F[_], O, E, S](eof: => E): EitherPullState[F, O, E, S, S, Byte, Chunk[Byte]] =
    for
      n <- uncons1[F, O, E, S, Byte](eof)
      chunk <- unconsN[F, O, E, S, Byte](n)(eof)
    yield
      chunk

  def uncons[F[_], O, E, S, I](eof: => E): EitherPullState[F, O, E, S, S, I, Chunk[I]] =
    EitherPullState((s, stream) => stream.pull.uncons.map(_.toRight(eof).map((chunk, tail) => ((s, tail), chunk))))

  def uncons1[F[_], O, E, S, I](eof: => E): EitherPullState[F, O, E, S, S, I, I] =
    EitherPullState((s, stream) => stream.pull.uncons1.map(_.toRight(eof).map((i, tail) => ((s, tail), i))))

  def unconsLimit[F[_], O, E, S, I](n: Int)(eof: => E): EitherPullState[F, O, E, S, S, I, Chunk[I]] =
    EitherPullState((s, stream) => stream.pull.unconsLimit(n).map(_.toRight(eof)
      .map((chunk, tail) => ((s, tail), chunk)))
    )

  def unconsMin[F[_], O, E, S, I](n: Int, allowFewerTotal: Boolean = false)(eof: => E)
  : EitherPullState[F, O, E, S, S, I, Chunk[I]] =
    EitherPullState((s, stream) => stream.pull.unconsMin(n, allowFewerTotal).map(_.toRight(eof)
      .map((chunk, tail) => ((s, tail), chunk)))
    )

  def unconsN[F[_], O, E, S, I](n: Int, allowFewer: Boolean = false)(eof: => E)
  : EitherPullState[F, O, E, S, S, I, Chunk[I]] =
    EitherPullState((s, stream) => stream.pull.unconsN(n, allowFewer).map(_.toRight(eof)
      .map((chunk, tail) => ((s, tail), chunk)))
    )

  def unit[F[_], O, E, S, I]: EitherPullState[F, O, E, S, S, I, Unit] = IndexedStateT.pure(())

  def liftF[F[_], O, E, S, I, A](f: Pull[F, O, Either[E, A]]): EitherPullState[F, O, E, S, S, I, A] =
    IndexedStateT.liftF(f)

  def liftEither[F[_], O, E, S, I, A](either: Either[E, A]): EitherPullState[F, O, E, S, S, I, A] =
    liftF(Pull.pure(either))
end EitherPullState
