package wiki.extractor.util

import io.airlift.compress.zstd.{ZstdCompressor, ZstdDecompressor}

import java.nio.charset.StandardCharsets

object Compressor {

  // Compress an input string to a ZStandard byte array
  def compress(input: String): Array[Byte] = {
    val stringBytes = input.getBytes(StandardCharsets.UTF_8)
    require(stringBytes.length < maxSize)
    val maxCompressedLength = compressor.maxCompressedLength(stringBytes.length)
    val buffer              = new Array[Byte](maxCompressedLength)
    val compressedSize      = compressor.compress(stringBytes, 0, stringBytes.length, buffer, 0, buffer.length)
    buffer.take(compressedSize)
  }

  // Compress an input byte array to a ZStandard byte array
  def compress(input: Array[Byte]): Array[Byte] = {
    require(input.length < maxSize)
    val maxCompressedLength = compressor.maxCompressedLength(input.length)
    val buffer              = new Array[Byte](maxCompressedLength)
    val compressedSize      = compressor.compress(input, 0, input.length, buffer, 0, buffer.length)
    buffer.take(compressedSize)
  }

  // Decompress a ZStandard byte array to an uncompressed byte array
  def decompress(input: Array[Byte]): Array[Byte] = {
    require(input.length < maxSize)
    val decompressedSize = ZstdDecompressor.getDecompressedSize(input, 0, maxSize)
    val buffer           = new Array[Byte](decompressedSize.toInt)
    decompressor.decompress(input, 0, input.length, buffer, 0, buffer.length)
    buffer
  }

  // Restore a string created by compress
  def decompressToString(input: Array[Byte]): String = {
    require(input.length < maxSize)
    val decompressedSize = ZstdDecompressor.getDecompressedSize(input, 0, maxSize)
    val buffer           = new Array[Byte](decompressedSize.toInt)
    decompressor.decompress(input, 0, input.length, buffer, 0, buffer.length)
    new String(buffer, StandardCharsets.UTF_8)
  }

  private val compressor   = new ZstdCompressor
  private val decompressor = new ZstdDecompressor
  private val maxSize      = Int.MaxValue / 2
}
