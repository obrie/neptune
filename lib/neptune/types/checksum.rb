require 'zlib'
require 'neptune/errors'
require 'neptune/types/int32'
require 'neptune/types/size'

module Neptune
  module Types
    # Provides typecasting for Checksum data
    class Checksum
      # Converts the given value to its Kafka format
      # @return [String]
      def self.to_kafka(value, buffer)
        checksum = Zlib.crc32(buffer.peek(buffer.size))
        result = Int32.to_kafka(checksum)
        result.prepend(Int32.to_kafka(buffer.size + result.size))
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Fixnum]
      def self.from_kafka(buffer)
        # Validate enough bytes to read size / checksum
        throw :halt if buffer.bytes_remaining < 8

        # Validate enough bytes to read checksum + value
        size = Size.from_kafka(buffer)
        throw :halt if buffer.bytes_remaining < size

        # Validate checksum matches
        checksum = Int32.from_kafka(buffer)
        expected_checksum = [Zlib.crc32(buffer.peek(size - 4))].pack('l>').unpack('l>').first
        raise DecodingError.new("Checksum failed; expected #{expected_checksum}, got #{checksum}") if checksum != expected_checksum

        checksum
      end
    end
  end
end