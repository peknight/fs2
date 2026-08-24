package com.peknight.fs2.pull.state

/**
 * 字节进字节出版 [[ByteInPullStateErrorDsl]]：同时具备 [[BytePullStateDsl]] 的
 * `ByteVector` 输出便利方法与 [[PullStateErrorDsl]] 的 `Throwable => E` 默认提升。
 * 面向"字节进、字节出"且错误可由 `State + Throwable` 构造的线协议（如 SOCKS5）。
 */
trait BytePullStateErrorDsl[S, E]
  extends BytePullStateDsl[S, E] with ByteInPullStateErrorDsl[Byte, S, E]
