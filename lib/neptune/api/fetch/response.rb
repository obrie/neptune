require 'neptune/response'
require 'neptune/api/fetch/topic_response'

module Neptune
  module Api
    module Fetch
      class Response < Neptune::Response
        # Responses for each topic within the cluster
        # @return [Array<Neptune::Api::Fetch::TopicResponse>]
        attribute :topic_responses, ArrayOf[TopicResponse]

        def initialize(*) #:nodoc:
          super
          @topic_responses ||= []
        end

        # Whether all messages were successfully produced
        # @return [Boolean]
        def success?
          topic_responses.all? {|response| response.success?}
        end

        # The first available error in the response
        # @return [Fixnum]
        def error_code
          topic_responses.map(&:error_code).compact.first
        end

        # Messages for the requested topics / partitions
        # @return [Array<Neptune::Message>]
        def messages
          topic_responses.map {|response| response.partition_responses.map(&:messages)}.flatten
        end
      end
    end
  end
end