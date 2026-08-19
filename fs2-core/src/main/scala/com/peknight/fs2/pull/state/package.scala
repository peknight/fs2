package com.peknight.fs2.pull

import cats.data.StateT
import fs2.{Pull, Stream}

package object state:
  type PullState[F[_], I, O, S, A] = StateT[[X] =>> Pull[F, O, X], (S, Stream[F, I]), A]
  type BytePullState[F[_], O, S, A] = PullState[F, Byte, O, S, A]
end state
