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

        delegate :each => :responses
      end
    end
  end
end