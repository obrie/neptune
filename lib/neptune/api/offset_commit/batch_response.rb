require 'neptune/response'
require 'neptune/api/offset_commit/response'

module Neptune
  module Api
    module OffsetCommit
      # A batch of one or more responses delineated by topic / partition
      class BatchResponse < Neptune::Response
        include Enumerable

        # Responses for each topic / partition within the cluster
        # @return [Array<Neptune::Api::OffsetCommit::Response>]
        attribute :responses, IndexedArrayOf[OffsetCommit::Response]

        def initialize(*) #:nodoc:
          super
          @responses ||= []
        end

        delegate [:each, :<<] => :responses
      end
    end
  end
end