package com.peknight.fs2.pull

import cats.data.StateT
import fs2.{Pull, Stream}

package object state:
  type PullState[F[_], I, O, S, E, A] = StateT[[X] =>> Pull[F, O, Either[E, X]], (S, Stream[F, I]), A]
end state
