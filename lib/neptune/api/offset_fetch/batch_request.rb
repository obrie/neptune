require 'neptune/request'
require 'neptune/api/offset_fetch/request'

module Neptune
  module Api
    module OffsetFetch
      # A batch of one or more requests delineated by topic / partition
      class BatchRequest < Neptune::Request
        # The name of the consumer group to fetch offsets for
        # @return [String]
        attribute :consumer_group, String

        # Requests to send for topics/partitions
        # @return [Array<Neptune::Api::OffsetFetch::Request>]
        attribute :requests, IndexedArrayOf[OffsetFetch::Request]

        def initialize(*) #:nodoc:
          super
          self.api_key = 9
        end
      end
    end
  end
end