package com.peknight.fs2.pull.state

import cats.ApplicativeError
import cats.data.StateT
import cats.syntax.applicative.*
import cats.syntax.applicativeError.*
import fs2.Stream.ToPull
import fs2.{Chunk, Pull, Stream}

import java.io.EOFException
import java.nio.charset.Charset

object PullState:
  def apply[F[_], I, O, A](f: Stream[F, I] => Pull[F, O, (Stream[F, I], A)]): PullState[F, I, O, A] = StateT(f)

  def pure[F[_], I, O, A](a: A): PullState[F, I, O, A] = StateT.pure(a)

  def unit[F[_], I, O]: PullState[F, I, O, Unit] = pure[F, I, O, Unit](())

  def liftF[F[_], I, O, A](f: Pull[F, O, A]): PullState[F, I, O, A] = StateT.liftF(f)

  def pull[F[_], I, O, A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])(eof: => Throwable)
                         (using ApplicativeError[F, Throwable]): PullState[F, I, O, A] =
    apply[F, I, O, A](stream => f(stream.pull).evalMap {
      case Some(tuple) => tuple.swap.pure[F]
      case _ => eof.raiseError[F, (Stream[F, I], A)]
    })

  def unconsSizedBytes[F[_], O](eof: => Throwable = new EOFException())(using ApplicativeError[F, Throwable])
  : BytePullState[F, O, Chunk[Byte]] =
    for
      n <- pull[F, Byte, O, Byte](_.uncons1)(eof)
      chunk <- pull[F, Byte, O, Chunk[Byte]](_.unconsN(n))(eof)
    yield
      chunk

  def unconsSizedString[F[_], O](eof: => Throwable = new EOFException())
                                (using charset: Charset)(using ApplicativeError[F, Throwable])
  : BytePullState[F, O, String] =
    for
      chunk <- unconsSizedBytes[F, O](eof)
      bytes = chunk.toByteVector
      value <-
        bytes.decodeString match
          case Right(value) => pure[F, Byte, O, String](value)
          case Left(e) => liftF[F, Byte, O, String](Pull.eval(e.raiseError[F, String]))
    yield
      value

end PullState
