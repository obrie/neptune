require 'neptune/request'
require 'neptune/api/produce/request'

module Neptune
  module Api
    module Produce
      # A batch of one or more requests delineated by topic / partition
      class BatchRequest < Neptune::Request
        # How many acknowledgements the servers should receive before responding to
        # the request
        # @return [Fixnum]
        attribute :required_acks, Int16

        # Maximum time, in milliseconds, the server can await the receipt of the
        # number of required acknowledgements
        # @return [Fixnum]
        attribute :ack_timeout, Int32

        # Requests to send for topics/partitions
        # @return [Array<Neptune::Api::Produce::Request>]
        attribute :requests, IndexedArrayOf[Produce::Request]

        def initialize(*) #:nodoc:
          super
          self.api_key = 0
        end
      end
    end
  end
end