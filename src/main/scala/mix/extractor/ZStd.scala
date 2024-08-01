package mix.extractor

import io.airlift.compress.zstd.{ZstdCompressor, ZstdDecompressor}

import scala.util.{Failure, Success, Try}

object ZStd {
  // Compress an input byte array using zstandard compression
  def compress(input: Array[Byte]): Array[Byte] = {
    require(input.length < maxSize)
    val compressor = new ZstdCompressor
    val maxCompressedLength = compressor.maxCompressedLength(input.length)
    val buffer = new Array[Byte](maxCompressedLength)
    val compressedSize = compressor.compress(input, 0, input.length, buffer, 0, buffer.length)
    buffer.take(compressedSize)
  }

  // Decompress an input byte array compressed as zstandard
  def decompress(input: Array[Byte]): Array[Byte] = {
    require(input.length < maxSize)
    Try {
      val decompressedSize = ZstdDecompressor.getDecompressedSize(input, 0, maxSize)
      val buffer = new Array[Byte](decompressedSize.toInt)
      val decompressor = new ZstdDecompressor
      decompressor.decompress(input, 0, input.length, buffer, 0, buffer.length)
      buffer
    } match {
      case Success(result) => result
      case Failure(ex) => throw new Exception(s"Compressed input could not be decompressed as zstd: $ex")
    }
  }

  // Don't try to handle more than 1 GB of uncompressed data
  private val maxSize = Int.MaxValue / 2
}
