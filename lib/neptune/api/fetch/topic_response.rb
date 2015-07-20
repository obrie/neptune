require 'neptune/resource'
require 'neptune/api/fetch/partition_response'

module Neptune
  module Api
    module Fetch
      class TopicResponse < Resource
        # The name of the topic this response is for
        # @return [String]
        attribute :topic_name, String

        # Responses by partition
        # @return [Array<Neptune::Api::Fetch::PartitionResponse>]
        attribute :partition_responses, ArrayOf[PartitionResponse]

        # Whether all partitions were fetched from successfully
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