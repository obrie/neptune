require 'neptune/resource'
require 'neptune/api/fetch/partition_request'

module Neptune
  module Api
    module Fetch
      class TopicRequest < Resource
        # The name of the topic being requested
        # @return [String]
        attribute :topic_name, String

        # Requests by partition
        # @return [Array<Neptune::Api::Fetch::PartitionRequest>]
        attribute :partition_requests, ArrayOf[PartitionRequest]
      end
    end
  end
end