package com.peknight.fs2

import _root_.fs2.Stream
import cats.effect.{Resource, Ref}

package object resource:
  def test[F[_], A, B](stream: Stream[F, A])(f: A => Resource[F, B]): Resource[F, Ref[F, B]] = ???
end resource