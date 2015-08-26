require 'neptune/request'
require 'neptune/api/offset_commit/request'

module Neptune
  module Api
    module OffsetCommit
      # A batch of one or more requests delineated by topic / partition
      class BatchRequest < Neptune::Request
        # The name of the consumer group to fetch offsets for
        # @return [String]
        attribute :consumer_group, String

        # The unique id identifyingg this generation of consumers
        # @return [Fixnum]
        attribute :consumer_group_generation_id, Int32, version: 1

        # The unique identifier assigned to this consumer by the group coordinator
        # @return [String]
        attribute :consumer_id, String, version: 1

        # The amount of time to retain the offset (in ms)
        # @return [Fixnum]
        attribute :retention_time, Int64, version: 2

        # Requests to send for topics/partitions
        # @return [Array<Neptune::Api::OffsetCommit::Request>]
        attribute :requests, IndexedArrayOf[OffsetCommit::Request]

        def initialize(*) #:nodoc:
          super
          self.api_key = 8
        end
      end
    end
  end
end