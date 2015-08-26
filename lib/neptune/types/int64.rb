module Neptune
  module Types
    # Provides typecasting for Int64 data
    class Int64
      # Converts the given value to its Kafka format
      # @return [String]
      def self.to_kafka(value, *)
        [value].pack('q>')
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Fixnum]
      def self.from_kafka(buffer, *)
        buffer.read(8).unpack('q>').first
      end
    end
  end
end