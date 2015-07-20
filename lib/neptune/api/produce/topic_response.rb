require 'neptune/resource'
require 'neptune/api/produce/partition_response'

module Neptune
  module Api
    module Produce
      class TopicResponse < Resource
        # The topic messages are being published to
        # @return [String]
        attribute :topic_name, String

        # Responses for each partition within the topic
        # @return [Array<Neptune::Api::Produce::PartitionResponse>]
        attribute :partition_responses, ArrayOf[PartitionResponse]

        # Whether all partitions were produced to successfully
        # @return [Boolean]
        def success?
          partition_responses.all? {|response| response.success?}
        end

        # The first available error in the response
        # @return [Fixnum]
        def error_code
          failed_response = partition_responses.reject(&:success?).first
          if failed_response
            failed_response.error_code
          end
        end
      end
    end
  end
end