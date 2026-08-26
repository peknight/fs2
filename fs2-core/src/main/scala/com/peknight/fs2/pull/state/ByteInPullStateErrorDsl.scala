package com.peknight.fs2.pull.state

import fs2.Chunk

import java.nio.charset.Charset

/**
 * 字节输入版 [[PullStateErrorDsl]]：在 [[ByteInputPullStateDsl]] 的字节读组合子基础上，
 * 提供接收 `eof: => Throwable` 的 `readSized*` / `mapSizedBytes` 重载，EOF 异常同样经
 * [[PullStateErrorDsl.error]] 提升为 `E`。
 */
trait ByteInPullStateErrorDsl[O, S, E]
  extends ByteInputPullStateDsl[O, S, E] with PullStateErrorDsl[Byte, O, S, E]:

  def readSizedBytes[F[_]](eof: => Throwable): Aux[F, Chunk[Byte]] =
    super.readSizedBytes(s => error(s, eof))

  def mapSizedBytes[F[_], A](f: Chunk[Byte] => A)(eof: => Throwable): Aux[F, A] =
    super.mapSizedBytes(f)(s => error(s, eof))

  def parseSizedBytes[F[_], A](f: Chunk[Byte] => Either[Throwable, A])(eof: => Throwable): Aux[F, A] =
    super.parseSizedBytes(f)(error)(s => error(s, eof))

  def readSizedString[F[_]](eof: => Throwable)(using Charset): Aux[F, String] =
    super.readSizedString(error)(s => error(s, eof))

  def mapSizedString[F[_], A](f: String => A)(eof: => Throwable)(using Charset): Aux[F, A] =
    super.mapSizedString(f)(error)(s => error(s, eof))

  def parseSizedString[F[_], A](f: String => Either[Throwable, A])(eof: => Throwable)(using Charset): Aux[F, A] =
    super.parseSizedString(f)(error)(s => error(s, eof))
end ByteInPullStateErrorDsl
