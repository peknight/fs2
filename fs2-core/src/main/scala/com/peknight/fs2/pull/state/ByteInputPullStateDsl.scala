package com.peknight.fs2.pull.state

import com.peknight.cats.instances.eitherT.given
import fs2.Chunk

import java.nio.charset.Charset

/**
 * 输入固定为 `Byte` 的 [[PullStateDsl]]，输出类型 `O` 仍保持自由。
 *
 * 承载只依赖"从字节流读取"的组合子（`readSized*` 等），因此"字节进、帧/对象出"的纯解码协议
 * 也可直接复用本层，无需把输出也约束成 `Byte`。
 */
trait ByteInputPullStateDsl[O, S, E] extends PullStateDsl[Byte, O, S, E]:

  /**
   * 先读一个长度字节，再按其无符号值读取对应长度的字节块。
   * SOCKS5 等协议的长度字段是 unsigned byte，必须做无符号拓宽（0-255），
   * 否则 128-255 会作为负数传给 `unconsN`。
   */
  def readSizedBytes[F[_]](eof: S => E): AUX[F, Chunk[Byte]] =
    for
      n <- pull[F, Byte](_.uncons1)(eof)
      chunk <- pull[F, Chunk[Byte]](_.unconsN(n & 0xFF))(eof)
    yield
      chunk

  def mapSizedBytes[F[_], A](f: Chunk[Byte] => A)(eof: S => E): AUX[F, A] =
    readSizedBytes(eof).map(f)

  def parseSizedBytes[F[_], A](f: Chunk[Byte] => Either[Throwable, A])
                              (error: (S, Throwable) => E)(eof: S => E): AUX[F, A] =
    for
      chunk <- readSizedBytes(eof)
      value <- liftET(f(chunk))(error)
    yield
      value

  def readSizedString[F[_]](error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : AUX[F, String] =
    parseSizedBytes[F, String](_.toByteVector.decodeString)(error)(eof)

  def mapSizedString[F[_], A](f: String => A)(error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : AUX[F, A] =
    readSizedString(error)(eof).map(f)

  def parseSizedString[F[_], A](f: String => Either[Throwable, A])
                               (error: (S, Throwable) => E)(eof: S => E)(using Charset)
  : AUX[F, A] =
    for
      value <- readSizedString(error)(eof)
      value <- liftET(f(value))(error)
    yield
      value
end ByteInputPullStateDsl
