package com.peknight.fs2.pull

import cats.data.IndexedStateT
import fs2.{Pull, Stream}

package object state:
  type EitherPullState[F[_], O, E, SA, SB, I, A] =
    IndexedStateT[[X] =>> Pull[F, O, Either[E, X]], (SA, Stream[F, I]), (SB, Stream[F, I]), A]
end state
