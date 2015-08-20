require 'neptune/api/fetch'
require 'neptune/api/metadata'
require 'neptune/api/produce'

module Neptune
  module Api
    NAMES = {
      fetch: Fetch,
      metadata: Metadata,
      produce: Produce
    }

    # Look up the Api module with the given name
    # @return [Module]
    def self.get(name)
      NAMES.fetch(name)
    end

    # Look up the Api name for the given module
    # @return [Symbol]
    def self.name_for(api)
      NAMES.key(api) || raise(KeyError.new("key not found: #{api.name}"))
    end
  end
end