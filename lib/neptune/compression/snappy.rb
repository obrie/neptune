require 'neptune/errors'

module Neptune
  module Compression
    # Snappy compression scheme for messages
    class Snappy
      # The unique id of this codec
      # @return [Fixnum]
      def self.id
        2
      end

      # The human-readable name of this codec
      # @return [String]
      def self.name
        :snappy
      end

      # Disables this compression scheme from being used
      def self.disable
        @enabled = false
      end
      @enabled = true

      # Compresses the given value
      # @return [String]
      def self.compress(value)
        check!
        ::Snappy.deflate(value)
      end

      # Decompresses the given value
      # @return [String]
      def self.decompress(value)
        check!
        ::Snappy::Reader.new(StringIO.new(value)).read
      end

      private
      def self.check! #:nodoc:
        raise Error.new('Snappy compression library unavailable')
      end
    end
  end
end

begin
  require 'snappy'
rescue LoadError
  # Library unavailable: disable
  Neptune::Compression::Snappy.disable
end