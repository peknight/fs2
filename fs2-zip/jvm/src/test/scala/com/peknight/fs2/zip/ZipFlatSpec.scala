package com.peknight.fs2.zip

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.text.utf8
import org.scalatest.flatspec.AsyncFlatSpec

class ZipFlatSpec extends AsyncFlatSpec with AsyncIOSpec:
  "Zip" should "pass" in {
    def write(path: Path, content: String): IO[Unit] =
      Stream[IO, String](content).through(utf8.encode[IO]).through(Files[IO].writeAll(path)).compile.drain
    def read(path: Path): IO[String] =
      Files[IO].readAll(path).through(utf8.decode[IO]).compile.toList.map(_.mkString.trim)
    val testDir: Path = Path("test")
    val dir: Path = Path("zip-dir")
    val file: Path = Path("zip-file.txt")
    val fileContent: String = "file1"
    val subFile1: Path = dir / Path("zip-sub-file-1.txt")
    val subFile1Content: String = "file2"
    val subFile2: Path = dir / Path("zip-sub-file-2.txt")
    val subFile2Content: String = "file3"
    val subDir: Path = dir / Path("zip-sub-dir")
    val subSubFile: Path = subDir / Path("zip-sub-sub-file.txt")
    val subSubFileContent: String = "file4"
    val zipFile: Path = testDir / Path("zip.zip")
    val outputDir: Path = testDir / Path("zip-output")
    for
      _ <- Files[IO].createDirectories(testDir / subDir)
      _ <- write(testDir / file, fileContent)
      _ <- write(testDir / subFile1, subFile1Content)
      _ <- write(testDir / subFile2, subFile2Content)
      _ <- write(testDir / subSubFile, subSubFileContent)
      _ <- Stream[IO, Path](testDir / dir, testDir / file)
        .through(readAll[IO])
        .through(archive[IO]())
        .through(Files[IO].writeAll(zipFile))
        .compile
        .drain
      _ <- Files[IO].readAll(zipFile)
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
end ZipFlatSpec
