package com.peknight.fs2.pull.state

import cats.data.StateT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.peknight.cats.instances.eitherT.given
import fs2.Stream.ToPull
import fs2.{Chunk, Pipe, Pull, Stream}

import scala.reflect.ClassTag

/**
 * 固定输入 `I`、输出 `O`、状态 `S`、错误 `E` 四个类型参数后的 [[PullState]] 构造器与扩展方法集合。
 *
 * `F[_]` 与返回值 `A` 仍保留在各方法上，因此调用处既能写出 `dsl.getS[F]` 这样的短签名，
 * 又不丢失 [[PullState]] 六泛型在底层的全部灵活性。新增协议/流形态时：
 *   - 字节输入、自由输出：继承 [[ByteInputPullStateDsl]]
 *   - 字节输入、字节输出：继承 [[BytePullStateDsl]]
 *   - 其它 `I/O`：直接继承本类
 *
 * 所有构造逻辑直接在本类中实现，不再转发到独立的 `object PullState`；
 * 需要六泛型完全开放的场景时，`new PullStateDsl[I, O, S, E] {}` 即可获得同一套 API。
 */
trait PullStateDsl[I, O, S, E]:

  /** 特化后的 StateT 类型；叶子 object 可再将其 type alias 成协议自有的 `XxxPullState[F, A]`。 */
  final type PS[F[_], A] = PullState[F, I, O, S, E, A]

  private type P[F[_]] = [X] =>> Pull[F, O, Either[E, X]]
  private type ST[F[_]] = (S, Stream[F, I])

  def apply[F[_], A](f: (S, Stream[F, I]) => Pull[F, O, Either[E, ((S, Stream[F, I]), A)]])
  : PS[F, A] =
    StateT[P[F], ST[F], A](f.tupled)

  def pure[F[_], A](a: A): PS[F, A] = StateT.pure[P[F], ST[F], A](a)

  def unit[F[_]]: PS[F, Unit] = pure(())

  def get[F[_]]: PS[F, ST[F]] = StateT.get[P[F], ST[F]]

  def getS[F[_]]: PS[F, S] = get.map(_._1)

  def setS[F[_]](s: S): PS[F, Unit] =
    for
      (_, stream) <- get
      _ <- StateT.set[P[F], ST[F]]((s, stream))
    yield
      ()

  def liftPE[F[_], A](pull: Pull[F, O, Either[E, A]]): PS[F, A] = StateT.liftF(pull)
  def liftP[F[_], A](pull: Pull[F, O, A]): PS[F, A] = liftPE(pull.map(_.asRight[E]))
  def liftPL[F[_], A](pull: Pull[F, O, E]): PS[F, A] = liftPE(pull.map(_.asLeft[A]))

  def liftFE[F[_], A](f: F[Either[E, A]]): PS[F, A] = liftPE(Pull.eval(f))
  def liftF[F[_], A](f: F[A]): PS[F, A] = liftP(Pull.eval(f))
  def liftFL[F[_], A](f: F[E]): PS[F, A] = liftPL(Pull.eval(f))

  def liftE[F[_], A](either: Either[E, A]): PS[F, A] =
    either match
      case Right(value) => pure(value)
      case Left(error) => liftL(error)
  def liftL[F[_], A](e: E): PS[F, A] = liftPL(Pull.pure(e))

  def liftPET[F[_], A](pull: Pull[F, O, Either[Throwable, A]])(error: (S, Throwable) => E)
  : PS[F, A] =
    liftP[F, Either[Throwable, A]](pull).flatMap {
      case Right(a) => pure(a)
      case Left(e) => getS.flatMap(state => liftL(error(state, e)))
    }
  def liftPLT[F[_], A](pull: Pull[F, O, Throwable])(error: (S, Throwable) => E)
  : PS[F, A] =
    liftPET(pull.map(_.asLeft[A]))(error)
  def liftFET[F[_], A](f: F[Either[Throwable, A]])(error: (S, Throwable) => E)
  : PS[F, A] =
    liftPET(Pull.eval(f))(error)
  def liftFLT[F[_], A](f: F[Throwable])(error: (S, Throwable) => E): PS[F, A] =
    liftPLT(Pull.eval(f))(error)
  def liftET[F[_], A](either: Either[Throwable, A])(error: (S, Throwable) => E)
  : PS[F, A] =
    liftPET(Pull.pure(either))(error)
  def liftT[F[_], A](t: Throwable)(error: (S, Throwable) => E): PS[F, A] =
    liftPLT(Pull.pure(t))(error)

  def output[F[_]](chunk: Chunk[O]): PS[F, Unit] = liftP(Pull.output(chunk))

  def output[F[_]](os: O*): PS[F, Unit] = output(Chunk(os*))

  def output1[F[_]](o: O): PS[F, Unit] = liftP(Pull.output1(o))

  def pipe[F[_]](pipe: Pipe[F, I, O]): PS[F, Unit] =
    apply((state, stream) => stream.through(pipe).pull.echo.as(((state, Stream.empty), ()).asRight[E]))

  def typed[F[_], A, B: ClassTag](a: A)(f: (S, A) => E): PS[F, B] =
    a match
      case b: B => pure(b)
      case a => getS.flatMap(s => liftL(f(s, a)))

  def typedS[F[_], A: ClassTag](f: S => E): PS[F, A] =
    getS.flatMap {
      case a: A => pure(a)
      case s => liftL(f(s))
    }

  def pull[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(A, Stream[F, I])]])(eof: S => E)
  : PS[F, A] =
    apply((s, stream) => f(stream.pull).flatMap {
      case Some((a, tail)) => Pull.pure(((s, tail), a).asRight)
      case _ => Pull.pure(eof(s).asLeft)
    })

  def map[F[_], I2, A](f: ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                      (g: I2 => A)(eof: S => E): PS[F, A] =
    pull(f)(eof).map(g)

  def map1[F[_], A](f: I => A)(eof: S => E): PS[F, A] =
    map[F, I, A](_.uncons1)(f)(eof)

  def mapChunk[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                       (g: Chunk[I] => A)(eof: S => E): PS[F, A] =
    map[F, Chunk[I], A](f)(g)(eof)

  def parse[F[_], I2, A](f: ToPull[F, I] => Pull[F, O, Option[(I2, Stream[F, I])]])
                        (g: I2 => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    for
      i <- pull(f)(eof)
      a <- liftET(g(i))(error)
    yield
      a

  def parse1[F[_], A](f: I => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    parse[F, I, A](_.uncons1)(f)(error)(eof)

  def parseChunk[F[_], A](f: ToPull[F, I] => Pull[F, O, Option[(Chunk[I], Stream[F, I])]])
                         (g: Chunk[I] => Either[Throwable, A])(error: (S, Throwable) => E)(eof: S => E)
  : PS[F, A] =
    parse[F, Chunk[I], A](f)(g)(error)(eof)

  extension [F[_], A] (state: PS[F, A])
    def attempt(error: (S, Throwable) => E): PS[F, A] =
      apply((s, stream) => state.run((s, stream)).attempt.map {
        case Right(Right(tuple)) => tuple.asRight[E]
        case Right(Left(e)) => e.asLeft[((S, Stream[F, I]), A)]
        case Left(t) => error(s, t).asLeft[((S, Stream[F, I]), A)]
      })
    def outputE(f: Either[E, A] => Chunk[O]): PS[F, A] =
      apply((s, stream) => state.run((s, stream)).flatMap {
        case Right(((s, stream), value)) => Pull.output(f(value.asRight[E])).as(((s, stream), value).asRight[E])
        case Left(e) => Pull.output(f(e.asLeft[A])).as(e.asLeft[((S, Stream[F, I]), A)])
      })
    def output(f: A => Chunk[O]): PS[F, A] =
      state.flatMap(a => this.output(f(a)).as(a))
    def outputL(f: E => Chunk[O]): PS[F, A] =
      outputE {
        case Right(a) => Chunk.empty
        case Left(error) => f(error)
      }
  end extension
end PullStateDsl
