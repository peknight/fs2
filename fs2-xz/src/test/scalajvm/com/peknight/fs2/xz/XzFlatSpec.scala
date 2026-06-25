package com.peknight.fs2.xz

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.peknight.fs2.tar.{archive, readAll, unarchive, writeAll}
import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.text.utf8
import org.scalatest.flatspec.AsyncFlatSpec

class XzFlatSpec extends AsyncFlatSpec with AsyncIOSpec:
  "XZ" should "pass" in {
    def write(path: Path, content: String): IO[Unit] =
      Stream[IO, String](content).through(utf8.encode[IO]).through(Files[IO].writeAll(path)).compile.drain
    def read(path: Path): IO[String] =
      Files[IO].readAll(path).through(utf8.decode[IO]).compile.toList.map(_.mkString.trim)
    val testDir: Path = Path("test")
    val dir: Path = Path("xz-dir")
    val file: Path = Path("xz-file.txt")
    val fileContent: String = "file1"
    val subFile1: Path = dir / Path("xz-sub-file-1.txt")
    val subFile1Content: String = "file2"
    val subFile2: Path = dir / Path("xz-sub-file-2.txt")
    val subFile2Content: String = "file3"
    val subDir: Path = dir / Path("xz-sub-dir")
    val subSubFile: Path = subDir / Path("xz-sub-sub-file.txt")
    val subSubFileContent: String = "file4"
    val tarFile: Path = testDir / Path("xz.tar.xz")
    val outputDir: Path = testDir / Path("xz-output")
    for
      _ <- Files[IO].createDirectories(testDir / subDir)
      _ <- write(testDir / file, fileContent)
      _ <- write(testDir / subFile1, subFile1Content)
      _ <- write(testDir / subFile2, subFile2Content)
      _ <- write(testDir / subSubFile, subSubFileContent)
      _ <- Stream[IO, Path](testDir / dir, testDir / file)
        .through(readAll[IO])
        .through(archive[IO]())
        .through(compress[IO]())
        .through(Files[IO].writeAll(tarFile))
        .compile
        .drain
      _ <- Files[IO].readAll(tarFile)
        .through(decompress[IO]())
        .through(unarchive[IO]())
        .through(writeAll[IO](outputDir, false))
        .compile
        .drain
      outputFileContent <- read(outputDir / file)
      outputSubFile1Content <- read(outputDir / subFile1)
      outputSubFile2Content <- read(outputDir / subFile2)
      outputSubSubFileContent <- read(outputDir / subSubFile)
    yield
      assert(outputFileContent === fileContent)
      assert(outputSubFile1Content === subFile1Content)
      assert(outputSubFile2Content === subFile2Content)
      assert(outputSubSubFileContent === subSubFileContent)
  }
end XzFlatSpec
