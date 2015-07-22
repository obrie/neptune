require 'neptune/types/array'
require 'neptune/types/string'

module Neptune
  module Types
    # Provides typecasting for Hashes
    class IndexedArrayOf < ArrayOf
      class << self
        # Builds an array tied to the given types
        # @return [Neptune::Types::IndexedArrayOf]
        def [](type)
          new(type)
        end
      end

      # The attribute to use for indexing
      # @return [Class]
      attr_reader :index_name

      # The type of indices
      # @return [Class]
      attr_reader :index_type

      def initialize(type) #:nodoc:
        super(type)
        @index_name, @index_type = self.type.attributes.detect {|attr, type| type.is_a?(Index)}
        @index_type = @index_type.type
      end

      # Converts the given value to its Kafka format
      # @return [String]
      def to_kafka(values, *args)
        buffer = Buffer.new

        # Add size of hash
        buffer.concat(Int32.to_kafka(values.size))

        # Write out each index / array combination
        values_by_index = values.group_by {|value| value.send(index_name)}
        values_by_index.each do |index_value, values|
          buffer.concat(index_type.to_kafka(index_value, *args))
          buffer.concat(super(values, *args))
        end

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Hash]
      def from_kafka(buffer)
        size = Int32.from_kafka(buffer)
        size.times.each_with_object([]) do |_, values|
          index_value = index_type.from_kafka(buffer)

          # Read the values array associated with the index
          results = super
          results.each {|result| result[index_name] = index_value}
          values.concat(results)
        end
      end
    end
  end
end