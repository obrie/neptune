module Neptune
  module Types
    # Provides typecasting for Arrays
    class Index
      class << self
        # Builds an Array tied to the given type
        # @return [Neptune::Types::Index]
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
      def to_kafka(*args)
        ''
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Array]
      def from_kafka(*args)
        ''
      end
    end
  end
end