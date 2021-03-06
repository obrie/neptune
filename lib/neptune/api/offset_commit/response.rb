require 'neptune/resource'

module Neptune
  module Api
    module OffsetCommit
      class Response < Resource
        # The topic this response corresponds to
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition this response corresponds to
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The error from this partition
        # @return [Neptune::ErrorCode]
        attribute :error_code, ErrorCode

        delegate [:success?, :retriable?] => :error_code
      end
    end
  end
end