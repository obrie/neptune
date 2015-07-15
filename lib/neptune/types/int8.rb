module Neptune
  module Types
    # Provides typecasting for Int8 data
    class Int8
      # Converts the given value to its Kafka format
      # @return [String]
      def self.to_kafka(value, *)
        [value].pack('C')
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Fixnum]
      def self.from_kafka(buffer)
        buffer.read(1).unpack('C').first
      end
    end
  end
end