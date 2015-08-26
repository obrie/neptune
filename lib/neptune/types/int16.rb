module Neptune
  module Types
    # Provides typecasting for Int16 data
    class Int16
      # Converts the given value to its Kafka format
      # @return [String]
      def self.to_kafka(value, *)
        [value].pack('s>')
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Fixnum]
      def self.from_kafka(buffer, *)
        buffer.read(2).unpack('s>').first
      end
    end
  end
end