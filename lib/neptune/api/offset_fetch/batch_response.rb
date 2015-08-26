require 'neptune/response'
require 'neptune/api/offset_fetch/response'

module Neptune
  module Api
    module OffsetFetch
      # A batch of one or more responses delineated by topic / partition
      class BatchResponse < Neptune::Response
        include Enumerable

        # Responses for each topic / partition within the cluster
        # @return [Array<Neptune::Api::OffsetFetch::Response>]
        attribute :responses, IndexedArrayOf[OffsetFetch::Response]

        def initialize(*) #:nodoc:
          super
          @responses ||= []
        end

        delegate [:each, :<<] => :responses
      end
    end
  end
end