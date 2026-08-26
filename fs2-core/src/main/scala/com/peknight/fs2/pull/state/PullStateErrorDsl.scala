package com.peknight.fs2.pull.state

import fs2.{Chunk, Pull, Stream}

/**
 * 在 [[PullStateDsl]] 基础上约定一种固定的错误提升方式：任何 `Throwable` 都通过抽象方法
 * [[error]] 由当前状态 `S` 转换为协议错误 `E`。
 *
 * 混入本 trait 后，`liftPET`/`liftET`/`liftT` 等不再需要逐调用传入 `(S, Throwable) => E`，
 * `pull`/`parse` 等也可直接接收 `eof: => Throwable`，把每个叶子协议里重复的样板方法统一上提。
 */
trait PullStateErrorDsl[I, O, S, E] extends PullStateDsl[I, O, S, E]:

  /** 将底层 `Throwable` 结合当前状态转换为协议错误 `E`。 */
  def error(state: S, throwable: Throwable): E

  def liftPET[F[_], A](pull: Pull[F, O, Either[Throwable, A]]): Aux[F, A] =
    super.liftPET(pull)(error)
  def liftPLT[F[_], A](pull: Pull[F, O, Throwable]): Aux[F, A] =
    super.liftPLT(pull)(error)
  def liftFET[F[_], A](f: F[Either[Throwable, A]]): Aux[F, A] =
    super.liftFET(f)(error)
  def liftFLT[F[_], A](f: F[Throwable]): Aux[F, A] =
    super.liftFLT(f)(error)
  def liftET[F[_], A](either: Either[Throwable, A]): Aux[F, A] =
    super.liftET(either)(error)
  def liftT[F[_], A](t: Throwable): Aux[F, A] =
    super.liftT(t)(error)

  def pull[F[_], A](f: Stream.ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])(eof: => Throwable)
  : Aux[F, A] =
    super.pull(f)(s => error(s, eof))

  def map[F[_], I2, A](f: Stream.ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                      (g: I2 => A)(eof: => Throwable): Aux[F, A] =
    super.map(f)(g)(s => error(s, eof))

  def map1[F[_], A](f: I => A)(eof: => Throwable): Aux[F, A] =
    super.map1(f)(s => error(s, eof))

  def mapChunk[F[_], A](f: Stream.ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                       (g: Chunk[I] => A)(eof: => Throwable): Aux[F, A] =
    super.mapChunk(f)(g)(s => error(s, eof))

  def parse[F[_], I2, A](f: Stream.ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                        (g: I2 => Either[Throwable, A])(eof: => Throwable): Aux[F, A] =
    super.parse(f)(g)(error)(s => error(s, eof))

  def parse1[F[_], A](f: I => Either[Throwable, A])(eof: => Throwable): Aux[F, A] =
    super.parse1(f)(error)(s => error(s, eof))

  def parseChunk[F[_], A](f: Stream.ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                         (g: Chunk[I] => Either[Throwable, A])(eof: => Throwable): Aux[F, A] =
    super.parseChunk(f)(g)(error)(s => error(s, eof))

  extension [F[_], A] (state: Aux[F, A])
    /** 无参版本：用 [[error]] 把底层 throwable 提升为 `E`。 */
    def attempt: Aux[F, A] = super[PullStateDsl].attempt(state)(error)
  end extension
end PullStateErrorDsl
