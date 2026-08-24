package com.peknight.fs2.pull.state

import com.peknight.fs2.pull.state.PullState.{attempt as pullStateAttempt, output as pullStateOutput, outputE as pullStateOutputE, outputL as pullStateOutputL}
import fs2.Stream.ToPull
import fs2.{Chunk, Pipe, Pull, Stream}

import scala.reflect.ClassTag

/**
 * 固定输入 `I`、输出 `O`、状态 `S`、错误 `E` 四个类型参数后的 [[PullState]] 构造器集合。
 *
 * `F[_]` 与返回值 `A` 仍保留在各方法上，因此调用处既能写出 `dsl.getS[F]` 这样的短签名，
 * 又不丢失 [[PullState]] 六泛型在底层的全部灵活性。新增协议/流形态时：
 *   - 字节输入、自由输出：继承 [[ByteInPullStateDsl]]
 *   - 字节输入、字节输出：继承 [[BytePullStateDsl]]
 *   - 其它 `I/O`：直接继承本类
 */
abstract class PullStateDsl[I, O, S, E]:

  /** 特化后的 StateT 类型；叶子 object 可再将其 type alias 成协议自有的 `XxxPullState[F, A]`。 */
  final type PS[F[_], A] = PullState[F, I, O, S, E, A]

  def apply[F[_], A](f: (S, Stream[F, I]) => Pull[F, O, Either[E, ((S, Stream[F, I]), A)]])
  : PS[F, A] =
    PullState[F, I, O, S, E, A](f)

  def pure[F[_], A](a: A): PS[F, A] = PullState.pure[F, I, O, S, E, A](a)

  def unit[F[_]]: PS[F, Unit] = PullState.unit[F, I, O, S, E]

  def get[F[_]]: PS[F, (S, Stream[F, I])] = PullState.get[F, I, O, S, E]

  def getS[F[_]]: PS[F, S] = PullState.getS[F, I, O, S, E]

  def setS[F[_]](s: S): PS[F, Unit] = PullState.setS[F, I, O, S, E](s)

  def liftPE[F[_], A](pull: Pull[F, O, Either[E, A]]): PS[F, A] =
    PullState.liftPE[F, I, O, S, E, A](pull)
  def liftP[F[_], A](pull: Pull[F, O, A]): PS[F, A] =
    PullState.liftP[F, I, O, S, E, A](pull)
  def liftPL[F[_], A](pull: Pull[F, O, E]): PS[F, A] =
    PullState.liftPL[F, I, O, S, E, A](pull)

  def liftFE[F[_], A](f: F[Either[E, A]]): PS[F, A] =
    PullState.liftFE[F, I, O, S, E, A](f)
  def liftF[F[_], A](f: F[A]): PS[F, A] =
    PullState.liftF[F, I, O, S, E, A](f)
  def liftFL[F[_], A](f: F[E]): PS[F, A] =
    PullState.liftFL[F, I, O, S, E, A](f)

  def liftE[F[_], A](either: Either[E, A]): PS[F, A] =
    PullState.liftE[F, I, O, S, E, A](either)
  def liftL[F[_], A](e: E): PS[F, A] = PullState.liftL[F, I, O, S, E, A](e)

  def liftPET[F[_], A](pull: Pull[F, O, Either[Throwable, A]])(error: (S, Throwable) => E)
  : PS[F, A] =
    PullState.liftPET[F, I, O, S, E, A](pull)(error)
  def liftPLT[F[_], A](pull: Pull[F, O, Throwable])(error: (S, Throwable) => E)
  : PS[F, A] =
    PullState.liftPLT[F, I, O, S, E, A](pull)(error)
  def liftFET[F[_], A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : PS[F, A] =
    PullState.liftFET[F, I, O, S, E, A](f)(error)
  def liftFLT[F[_], A](f: F[Throwable])(error: (S, Throwable) => E): PS[F, A] =
    PullState.liftFLT[F, I, O, S, E, A](f)(error)
  def liftET[F[_], A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : PS[F, A] =
    PullState.liftET[F, I, O, S, E, A](either)(error)
  def liftT[F[_], A](t: Throwable)(error: (S, Throwable) => E): PS[F, A] =
    PullState.liftT[F, I, O, S, E, A](t)(error)

  def output[F[_]](chunk: Chunk[O]): PS[F, Unit] =
    PullState.output[F, I, O, S, E](chunk)

  def output[F[_]](os: O*): PS[F, Unit] = PullState.output[F, I, O, S, E](os*)

  def output1[F[_]](o: O): PS[F, Unit] = PullState.output1[F, I, O, S, E](o)

  def pipe[F[_]](pipe: Pipe[F, I, O]): PS[F, Unit] =
    PullState.pipe[F, I, O, S, E](pipe)

  def typed[F[_], A, B: ClassTag](a: A)(f: (S, A) => E): PS[F, B] =
    PullState.typed[F, I, O, S, E, A, B](a)(f)

  def typedS[F[_], A: ClassTag](f: S => E): PS[F, A] =
    PullState.typedS[F, I, O, S, E, A](f)

  def pull[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])(eof: S => E)
  : PS[F, A] =
    PullState.pull[F, I, O, S, E, A](f)(eof)

  def map[F[_], I2, A](f: ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                      (g: I2 => A)(eof: S => E): PS[F, A] =
    PullState.map[F, I, I2, O, S, E, A](f)(g)(eof)

  def map1[F[_], A](f: I => A)(eof: S => E): PS[F, A] =
    PullState.map1[F, I, O, S, E, A](f)(eof)

  def mapChunk[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                       (g: Chunk[I] => A)(eof: S => E): PS[F, A] =
    PullState.mapChunk[F, I, O, S, E, A](f)(g)(eof)

  def parse[F[_], I2, A](f: ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                        (g: I2 => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    PullState.parse[F, I, I2, O, S, E, A](f)(g)(error)(eof)

  def parse1[F[_], A](f: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    PullState.parse1[F, I, O, S, E, A](f)(error)(eof)

  def parseChunk[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                         (g: Chunk[I] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    PullState.parseChunk[F, I, O, S, E, A](f)(g)(error)(eof)

  extension [F[_], A] (state: PS[F, A])
    def attempt(error: (S, Throwable) => E): PS[F, A] = state.pullStateAttempt(error)
    def outputE(f: Either[E, A] => Chunk[O]): PS[F, A] = state.pullStateOutputE(f)
    def output(f: A => Chunk[O]): PS[F, A] = state.pullStateOutput(f)
    def outputL(f: E => Chunk[O]): PS[F, A] = state.pullStateOutputL(f)
  end extension
end PullStateDsl
