require 'neptune/response'
require 'neptune/api/offset/response'

module Neptune
  module Api
    module Offset
      # A batch of one or more responses delineated by topic / partition
      class BatchResponse < Neptune::Response
        include Enumerable

        # Responses for each topic / partition within the cluster
        # @return [Array<Neptune::Api::Offset::Response>]
        attribute :responses, IndexedArrayOf[Offset::Response]

        def initialize(*) #:nodoc:
          super
          @responses ||= []
        end

        delegate [:each, :<<] => :responses
      end
    end
  end
end