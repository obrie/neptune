require 'neptune/response'
require 'neptune/api/produce/response'

module Neptune
  module Api
    module Produce
      # A batch of one or more responses delineated by topic / partition
      class BatchResponse < Neptune::Response
        include Enumerable

        # Responses for each topic / partition within the cluster
        # @return [Array<Neptune::Api::Produce::Response>]
        attribute :responses, IndexedArrayOf[Produce::Response]

        def initialize(*) #:nodoc:
          super
          @responses ||= []
        end

        # Whether all messages were successfully produced
        # @return [Boolean]
        def success?
          responses.all? {|response| response.success?}
        end

        # The first available error in the response
        # @return [Fixnum]
        def error_code
          responses.map(&:error_code).compact.first
        end

        # Iterates over the underlying responses
        def each(&block)
          responses.each(&block)
        end
      end
    end
  end
end