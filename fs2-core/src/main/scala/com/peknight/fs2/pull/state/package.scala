package com.peknight.fs2.pull

import cats.data.StateT
import fs2.{Pull, Stream}

package object state:
  type PullState[F[_], I, O, A] = StateT[[X] =>> Pull[F, O, X], Stream[F, I], A]
  type BytePullState[F[_], O, A] = PullState[F, Byte, O, A]
end state
