require 'neptune/request'

module Neptune
  module Api
    module Fetch
      class PartitionRequest < Neptune::Resource
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