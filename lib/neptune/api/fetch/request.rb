require 'neptune/resource'

module Neptune
  module Api
    module Fetch
      class Request < Resource
        # The topic to fetch from
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition to fetch from
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The offset to begin from
        # @return [Fixnum]
        attribute :offset, Int64

        # The maximum bytes to include in messages for this partition
        # @return [Fixnum]
        attribute :max_bytes, Int32
      end
    end
  end
end