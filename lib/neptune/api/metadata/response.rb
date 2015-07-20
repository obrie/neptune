require 'neptune/broker'
require 'neptune/response'
require 'neptune/topic'

module Neptune
  module Api
    module Metadata
      class Response < Neptune::Response
        # Brokers used for the requested topics
        # @return [Array<Neptune::Broker]
        attribute :brokers, ArrayOf[Broker]

        # Topics found in the cluster
        # @return [Array<Neptune::Topic]
        attribute :topics, ArrayOf[Topic]
      end
    end
  end
end