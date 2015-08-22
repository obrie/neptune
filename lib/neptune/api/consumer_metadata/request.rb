require 'neptune/request'

module Neptune
  module Api
    module ConsumerMetadata
      class Request < Neptune::Request
        # The unqiue id of the consumer for managing offsets
        # @return [Fixnum]
        attribute :consumer_group, String

        def initialize(*) #:nodoc:
          super
          self.api_key = 10
        end
      end
    end
  end
end