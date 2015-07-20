require 'neptune/resource'
require 'neptune/api/produce/partition_request'

module Neptune
  module Api
    module Produce
      class TopicRequest < Resource
        # The name of the topic this message belongs to
        # @return [String]
        attribute :topic_name, String

        # Messages by partition
        # @return [Array<Neptune::Api::Produce::PartitionRequest>]
        attribute :partition_requests, ArrayOf[PartitionRequest]
      end
    end
  end
end