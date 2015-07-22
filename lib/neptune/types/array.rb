require 'neptune/types/int32'
require 'neptune/types/string'

module Neptune
  module Types
    # Provides typecasting for Arrays
    class ArrayOf
      class << self
        # Builds an Array tied to the given type
        # @return [Neptune::Types::ArrayOf]
        def [](type)
          new(type)
        end
      end

      # The type of values stored in the array
      # @return [Class]
      attr_reader :type

      def initialize(type) #:nodoc:
        type = Types::String if type == ::String
        @type = type
      end

      # Converts the given value to its Kafka format
      # @return [String]
      def to_kafka(values, *args)
        buffer = Buffer.new

        # Add size of array
        buffer.concat(Int32.to_kafka(values.size))

        # Add individual elements
        values.each {|value| buffer.concat(type.to_kafka(value, *args))}

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Array]
      def from_kafka(buffer)
        size = Int32.from_kafka(buffer)
        size.times.map { type.from_kafka(buffer) }
      end
    end

    # Provides typecasting for Arrays bound by bytesize
    class SizeBoundArrayOf < ArrayOf
      # Converts the given value to its Kafka format
      # @return [String]
      def to_kafka(values, *)
        buffer = Buffer.new

        # Add individual elements
        values.each {|value| buffer.concat(type.to_kafka(value))}

        # Add bytesize
        buffer.prepend(Int32.to_kafka(buffer.size))

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Array]
      def from_kafka(buffer)
        # Validate enough bytes to read size
        throw :truncated if buffer.bytes_remaining < 4

        # Validate enough bytes to read value
        size = Int32.from_kafka(buffer)
        throw :truncated if buffer.bytes_remaining < size

        array_buffer = Buffer.new(buffer.read(size))

        values = []
        while !array_buffer.eof? && (value = type.from_kafka(array_buffer))
          values << value
        end
        values
      end
    end
  end
end