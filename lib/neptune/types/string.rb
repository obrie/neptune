module Neptune
  module Types
    # Provides typecasting for String data
    class String
      # Converts the given value to its Kafka format
      def self.to_kafka(value)
        if value.nil?
          Int16.to_kafka(-1)
        else
          Int16.to_kafka(value.bytesize) << value
        end
      end

      # Converts from the Kafka data in the current buffer's position
      def self.from_kafka(buffer)
        length = Int16.from_kafka(buffer)
        buffer.read(length)
      end
    end
  end
end