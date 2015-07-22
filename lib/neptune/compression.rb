require 'neptune/compression/gzip'
require 'neptune/compression/snappy'

module Neptune
  module Compression
    class << self
      # Looks up the compression codec associated with the given name
      # @return [Class]
      def find_by_name(name)
        @codecs_by_name.fetch(name)
      end

      # Looks up the compression codec associated with the given id
      # @return [Class]
      def find_by_id(id)
        @codecs_by_id.fetch(id)
      end

      # Registers a new codec
      def register(codec)
        @codecs_by_name[codec.name] = codec
        @codecs_by_id[codec.id] = codec
      end
    end

    @codecs_by_name = {:none => nil}
    @codecs_by_id = {}

    register(Gzip)
    register(Snappy)
  end
end