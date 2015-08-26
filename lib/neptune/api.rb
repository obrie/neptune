require 'neptune/api/consumer_metadata'
require 'neptune/api/fetch'
require 'neptune/api/metadata'
require 'neptune/api/offset'
require 'neptune/api/offset_fetch'
require 'neptune/api/produce'

module Neptune
  module Api
    NAMES = {
      fetch: Fetch,
      metadata: Metadata,
      offset: Offset,
      offset_fetch: OffsetFetch,
      produce: Produce
    }

    class << self
      # Look up the Api module with the given name
      # @return [Module]
      def get(name)
        NAMES.fetch(name)
      end

      # Look up the Api module that the given class belongs to
      # @return [Module]
      def for_class(klass)
        const_get(klass.name.match(/^Neptune::Api::([^:]+)/)[1])
      end

      # Look up the Api name for the given module
      # @return [Symbol]
      def name_for(api)
        NAMES.key(api) || raise(KeyError.new("key not found: #{api.name}"))
      end
    end
  end
end