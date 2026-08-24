package com.peknight.fs2.pull.state

import com.peknight.fs2.pull.state.PullState.{output as pullStateOutput, outputE as pullStateOutputE, outputL as pullStateOutputL}
import fs2.Chunk
import scodec.bits.ByteVector

import scala.annotation.targetName

/**
 * 输入、输出均固定为 `Byte` 的 [[ByteInPullStateDsl]]，面向"字节进、字节出"的线协议。
 *
 * 在父类基础上补充 `ByteVector` 的输出便利方法。`Chunk[Byte]` 与 `ByteVector` 擦除后同为
 * `Object`，故 `ByteVector` 版本的扩展方法需用 `@targetName` 与继承来的 `Chunk` 版本区分 JVM 签名；
 * Scala 源码层仍统一以 `output`/`outputE`/`outputL` 调用，按参数类型重载。
 */
abstract class BytePullStateDsl[S, E] extends ByteInPullStateDsl[Byte, S, E]:

  def output[F[_]](bytes: ByteVector): PS[F, Unit] = output(Chunk.byteVector(bytes))

  extension [F[_], A] (state: PS[F, A])
    @targetName("outputEByteVector")
    def outputE(f: Either[E, A] => ByteVector): PS[F, A] =
      state.pullStateOutputE(f.andThen(Chunk.byteVector))
    @targetName("outputByteVector")
    def output(f: A => ByteVector): PS[F, A] =
      state.pullStateOutput(f.andThen(Chunk.byteVector))
    @targetName("outputLByteVector")
    def outputL(f: E => ByteVector): PS[F, A] =
      state.pullStateOutputL(f.andThen(Chunk.byteVector))
  end extension
end BytePullStateDsl
