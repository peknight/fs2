package com.peknight.fs2.pull.state

import fs2.Chunk
import scodec.bits.ByteVector

import scala.annotation.targetName

/**
 * 输入、输出均固定为 `Byte` 的 [[ByteInputPullStateDsl]]，面向"字节进、字节出"的线协议。
 *
 * 在父类基础上补充 `ByteVector` 的输出便利方法。`Chunk[Byte]` 与 `ByteVector` 擦除后同为
 * `Object`，故 `ByteVector` 版本的扩展方法需用 `@targetName` 与继承来的 `Chunk` 版本区分 JVM 签名；
 * Scala 源码层仍统一以 `output`/`outputE`/`outputL` 调用，按参数类型重载。
 */
trait BytePullStateDsl[S, E] extends ByteInputPullStateDsl[Byte, S, E]:

  def output[F[_]](bytes: ByteVector): AUX[F, Unit] = output(Chunk.byteVector(bytes))

  extension [F[_], A] (state: AUX[F, A])
    @targetName("outputEByteVector")
    def outputE(f: Either[E, A] => ByteVector): AUX[F, A] =
      super.outputE(state)(f.andThen(Chunk.byteVector))
    @targetName("outputByteVector")
    def output(f: A => ByteVector): AUX[F, A] =
      super.output(state)(f.andThen(Chunk.byteVector))
    @targetName("outputLByteVector")
    def outputL(f: E => ByteVector): AUX[F, A] =
      super.outputL(state)(f.andThen(Chunk.byteVector))
  end extension
end BytePullStateDsl
