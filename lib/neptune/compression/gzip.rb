require 'zlib'
require 'stringio'

module Neptune
  module Compression
    # Gzip compression scheme for messages
    class Gzip
      # The unique id of this codec
      # @return [Fixnum]
      def self.id
        1
      end

      # The human-readable name of this codec
      # @return [String]
      def self.name
        :gzip
      end

      # Compresses the given value
      # @return [String]
      def self.compress(value)
        io = StringIO.new
        io.set_encoding(Encoding::BINARY)
        gzip = Zlib::GzipWriter.new(io, Zlib::DEFAULT_COMPRESSION, Zlib::DEFAULT_STRATEGY)
        gzip.write(value)
        gzip.close
        io.string
      end

      # Decompresses the given value
      # @return [String]
      def self.decompress(value)
        io = StringIO.new(value)
        value = Zlib::GzipReader.new(io).read
        value.force_encoding(Encoding::BINARY)
      end
    end
  end
end
