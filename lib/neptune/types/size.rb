require 'neptune/types/int32'

module Neptune
  module Types
    # Provides typecasting for Checksum data
    class Size
      # Converts the given value to its Kafka format
      # @return [String]
      def self.to_kafka(value, buffer)
        Int32.to_kafka(buffer.size)
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Fixnum]
      def self.from_kafka(buffer)
        Int32.from_kafka(buffer)
      end
    end
  end
end