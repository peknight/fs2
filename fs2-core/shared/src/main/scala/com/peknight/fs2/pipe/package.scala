package com.peknight.fs2

import cats.Applicative
import cats.syntax.applicative.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import fs2.{Chunk, Pipe, Pull, Stream}

package object pipe:
  def allocate[F[_], O](f: (Chunk[O], Chunk[O]) => (Chunk[O], Chunk[O])): Pipe[F, O, O] =
    _.pull.scanChunks(Chunk.empty[O])(f).flatMap(Pull.output(_).void).stream

  def chunkTimesN[F[_], O](n: Int): Pipe[F, O, O] =
    require(n > 0, s"n must be positive, but was $n")
    allocate { (acc, hd) =>
      val headSize = hd.size
      val size = acc.size + headSize
      if size < n then (acc ++ hd, Chunk.empty[O])
      else if size % n == 0 then (Chunk.empty[O], acc ++ hd)
      else
        val (pfx, sfx) = hd.splitAt(headSize - size % n)
        (sfx, acc ++ pfx)
    }
  end chunkTimesN

  def scanS[F[_], I, I2 >: I, O, S](init: S)(f: (S, I2) => (S, O)): Pipe[F, I, O] =
    scanOpt[F, I, I2, O, S](init)(s => Some(i => f(s, i)))

  def scanOpt[F[_], I, I2 >: I, O, S](init: S)(f: S => Option[I2 => (S, O)]): Pipe[F, I, O] =
    def go(acc: S, s: Stream[F, I]): Pull[F, O, Unit] =
      f(acc) match
        case None => Pull.pure(())
        case Some(g) => s.pull.uncons1.flatMap {
          case Some((hd, tl)) =>
            val (s2, o) = g(hd)
            Pull.output1(o) >> go(s2, tl)
          case None => Pull.pure(())
        }
    in => go(init, in).stream

  def scanChunksInitLast[F[_], I, I2 >: I, O, S](initS: => S)(init: (S, Chunk[I2]) => (S, Chunk[O]))
                                                (last: (S, Chunk[I2]) => Chunk[O]): Pipe[F, I, O] =
    _.pull
      .scanChunks[(S, Chunk[I]), O]((initS, Chunk.empty[I])) { case ((current, acc), hd) =>
        if acc.isEmpty then ((current, hd), Chunk.empty[O])
        else
          val (next, os) = init(current, acc)
          ((next, hd), os)
      }
      .flatMap { case (current, acc) => Pull.output(last(current, acc)) }
      .stream
  end scanChunksInitLast

  def mapChunksInitLast[F[_], I, I2 >: I, O](init: Chunk[I2] => Chunk[O])(last: Chunk[I2] => Chunk[O]): Pipe[F, I, O] =
    _.pull.scanChunks[Chunk[I], O](Chunk.empty[I])((acc, hd) => (hd, init(acc)))
      .flatMap(acc => Pull.output(last(acc))).stream

  private def evalScanChunksOptPull[F[_], F2[x] >: F[x], I, I2 >: I, O, S](acc: S, s: Stream[F, I])
    (f: S => Option[Chunk[I2] => F2[(S, Chunk[O])]]): Pull[F2, O, S] =
    f(acc) match
      case None => Pull.pure(acc)
      case Some(g) => s.pull.uncons.flatMap {
        case Some((hd, tl)) => Pull.eval(g(hd)).flatMap { case (current, os) =>
          Pull.output(os) >> evalScanChunksOptPull(current, tl)(f)
        }
        case None => Pull.pure(acc)
      }
  end evalScanChunksOptPull

  def evalScanChunksOpt[F[_], F2[x] >: F[x], I, I2 >: I, O, S](init: => S)
    (f: S => Option[Chunk[I2] => F2[(S, Chunk[O])]]): Stream[F, I] => Stream[F2, O] =
    in => evalScanChunksOptPull(init, in)(f).void.stream

  def evalScanChunks[F[_], F2[x] >: F[x], I, I2 >: I, O, S](init: => S)
    (f: (S, Chunk[I2]) => F2[(S, Chunk[O])]): Stream[F, I] => Stream[F2, O] =
    evalScanChunksOpt(init)(s => Some(c => f(s, c)))

  def evalMapChunks[F[_], F2[x] >: F[x], I, I2 >: I, O](f: Chunk[I2] => F2[Chunk[O]]): Stream[F, I] => Stream[F2, O] =
    in => in.chunks.flatMap(o => Stream.evalUnChunk(f(o)))

  def evalTapChunks[F[_], F2[x] >: F[x], I, I2 >: I, O](f: Chunk[I2] => F2[O]): Stream[F, I] => Stream[F2, I] =
    in => in.chunks.flatMap(is => Stream.eval(f(is)).as(is).flatMap(Stream.chunk))

  def evalScanChunksInitLast[F[_], F2[x] >: F[x] : Applicative, I, I2 >: I, O, S](initS: => S)
    (init: (S, Chunk[I2]) => F2[(S, Chunk[O])])
    (last: (S, Chunk[I2]) => F2[Chunk[O]]): Stream[F, I] => Stream[F2, O] = in =>
    evalScanChunksOptPull[F, F2, I, I2, O, (S, Chunk[I2])]((initS, Chunk.empty[I2]), in) {
      case (current, acc) => Some { hd =>
        if acc.isEmpty then ((current, hd), Chunk.empty[O]).pure[F2]
        else
          init(current, acc).map { case (next, os) => ((next, hd), os) }
      }
    }.flatMap { case (current, acc) => Pull.eval(last(current, acc)).flatMap(Pull.output) }.stream
  end evalScanChunksInitLast

  def evalMapChunksInitLast[F[_], F2[x] >: F[x], I, I2 >: I, O](init: Chunk[I2] => F2[Chunk[O]])
    (last: Chunk[I2] => F2[Chunk[O]]): Stream[F, I] => Stream[F2, O] =
    def go(acc: Chunk[I], s: Stream[F, I]): Pull[F2, O, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) => Pull.eval(init(acc)).flatMap(os => Pull.output(os) >> go(hd, tl))
        case None => Pull.eval(last(acc)).flatMap(os => Pull.output(os))
      }
    in => go(Chunk.empty[I], in).stream
  end evalMapChunksInitLast

  def evalTapChunksInitLast[F[_], F2[x] >: F[x], I, I2 >: I, O1, O2](init: Chunk[I2] => F2[O1])
    (last: Chunk[I2] => F2[O2]): Stream[F, I] => Stream[F2, I] =
    def go(acc: Chunk[I], s: Stream[F, I]): Pull[F2, I, Unit] =
      s.pull.uncons.flatMap {
        case Some((hd, tl)) => Pull.output(acc) >> Pull.eval(init(acc)) >> go(hd, tl)
        case None => Pull.output(acc) >> Pull.eval(last(acc)).void
      }
    in => go(Chunk.empty[I], in).stream
  end evalTapChunksInitLast

  def evalTapLastOpt[F[_], F2[x] >: F[x], I, I2 >: I, O2](f: Option[I2] => F2[O2])
  : Stream[F, I] => Stream[F2, I] =
    _.pull.scanChunks[Chunk[I], I](Chunk.empty[I])((acc, hd) => (hd, acc))
      .flatMap(acc => Pull.output(acc) >> Pull.eval(f(acc.last)).void)
      .stream

  def evalTapLast[F[_], F2[x] >: F[x] : Applicative, I, I2 >: I, O2](f: I2 => F2[O2]): Stream[F, I] => Stream[F2, I] =
    evalTapLastOpt[F, F2, I, I2, Option[O2]](_.traverse(f))
end pipe
