require 'neptune/request'
require 'neptune/api/offset/request'

module Neptune
  module Api
    module Offset
      # A batch of one or more requests delineated by topic / partition
      class BatchRequest < Neptune::Request
        # The node id of the replica initiating this request.  Always -1.
        # @return [Fixnum]
        attribute :replica_id, Int32

        # Requests to send for topics/partitions
        # @return [Array<Neptune::Api::Offset::Request>]
        attribute :requests, IndexedArrayOf[Offset::Request]

        def initialize(*) #:nodoc:
          super
          self.api_key = 2
          self.replica_id = -1
        end
      end
    end
  end
end