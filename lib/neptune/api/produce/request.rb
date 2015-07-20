require 'neptune/request'
require 'neptune/api/produce/topic_request'

module Neptune
  module Api
    module Produce
      class Request < Neptune::Request
        # How many acknowledgements the servers should receive before responding to
        # the request
        # @return [Fixnum]
        attribute :required_acks, Int16

        # Maximum time, in milliseconds, the server can await the receipt of the
        # number of required acknowledgements
        # @return [Fixnum]
        attribute :ack_timeout, Int32

        # The messages to send
        # @return [Array<Neptune::Api::Produce::TopicRequest>]
        attribute :topic_requests, ArrayOf[TopicRequest]

        def initialize(*) #:nodoc:
          super
          self.api_key = 0
        end
      end
    end
  end
end