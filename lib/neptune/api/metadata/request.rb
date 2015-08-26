require 'neptune/request'

module Neptune
  module Api
    module Metadata
      class Request < Neptune::Request
        # List of topics to fetch metadata for.  If no topics are specified,
        # metadat will be fetched for all known topics.
        # @return [Array<String>]
        attribute :topic_names, ArrayOf[String]

        def initialize(*) #:nodoc:
          super
          self.api_key = 3
        end
      end
    end
  end
end