require 'neptune/api/consumer_metadata'
require 'neptune/api/fetch'
require 'neptune/api/metadata'
require 'neptune/api/offset'
require 'neptune/api/offset_commit'
require 'neptune/api/offset_fetch'
require 'neptune/api/produce'

module Neptune
  module Api
    class << self
      # Look up the Api module with the given name
      # @return [Module]
      def get(name)
        @names.fetch(name)
      end

      # Look up the Api module that the given class belongs to
      # @return [Module]
      def for_class(klass)
        const_get(klass.name.match(/^Neptune::Api::([^:]+)/)[1])
      end

      # Look up the Api name for the given module
      # @return [Symbol]
      def name_for(api)
        @names.key(api) || raise(KeyError.new("key not found: #{api.name}"))
      end

      # Registers an Api module with the given name
      def register(name, api)
        @names[name] = api
      end
    end

    @names = {}

    register(:consumer_metadata, ConsumerMetadata)
    register(:fetch, Fetch)
    register(:metadata, Metadata)
    register(:offset, Offset)
    register(:offset_fetch, OffsetFetch)
    register(:offset_commit, OffsetCommit)
    register(:produce, Produce)
  end
end