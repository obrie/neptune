module Neptune
  module Types
    # Provides typecasting for Int32 data
    class Int32
      # Converts the given value to its Kafka format
      def self.to_kafka(value)
        [value].pack('l>')
      end

      # Converts from the Kafka data in the current buffer's position
      def self.from_kafka(buffer)
        buffer.read(4).unpack('l>').first
      end
    end
  end
end