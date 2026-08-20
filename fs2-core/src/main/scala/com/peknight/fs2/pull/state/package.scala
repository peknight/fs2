package com.peknight.fs2.pull

import cats.data.StateT
import fs2.{Pull, Stream}

package object state:
  type PullState[F[_], I, O, S, E, A] = StateT[[X] =>> Pull[F, O, Either[E, X]], (S, Stream[F, I]), A]
  type ByteInPullState[F[_], O, S, E, A] = PullState[F, Byte, O, S, E, A]
  type BytePullState[F[_], S, E, A] = ByteInPullState[F, Byte, S, E, A]
end state
