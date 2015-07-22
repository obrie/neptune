require 'neptune/request'
require 'neptune/api/fetch/request'

module Neptune
  module Api
    module Fetch
      # A batch of one or more requests delineated by topic / partition
      class BatchRequest < Neptune::Request
        # The node id of the replica initiating this request.  Always -1.
        # @return [Fixnum]
        attribute :replica_id, Int32

        # The maximum amount of time to block waiting if insufficient data is
        # available at the time the request is issued
        # @return [Fixnum]
        attribute :max_wait_time, Int32

        # The minimum number of bytes of messages that must be available to give a
        # response. If set to 0, the server will always respond immediately.
        # @return [Fixnum]
        attribute :min_bytes, Int32

        # Requests to send for topics/partitions
        # @return [Array<Neptune::Api::Fetch::Request>]
        attribute :requests, IndexedArrayOf[Fetch::Request]

        def initialize(*) #:nodoc:
          super
          self.api_key = 1
          self.replica_id = -1
        end
      end
    end
  end
end