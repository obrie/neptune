require 'neptune/types/int32'

module Neptune
  module Types
    # Provides typecasting for Byte data
    class Bytes
      # Converts the given value to its Kafka format
      def self.to_kafka(value)
        if value.nil?
          Int32.to_kafka(-1)
        else
          Int32.to_kafka(value.bytesize) << value
        end
      end

      # Converts from the Kafka data in the current buffer's position
      def self.from_kafka(buffer)
        length = Int32.from_kafka(buffer)
        if length == -1
          nil
        else
          buffer.read(length)
        end
      end
    end
  end
end