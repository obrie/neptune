require 'neptune/resource'

module Neptune
  module Api
    module Offset
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

        # The offsets requested
        # @return [Array<Fixnum>]
        attribute :offsets, ArrayOf[Int64]

        delegate [:success?, :retriable?] => :error_code

        # The value of the offset requested
        # @return [Fixnum]
        def value
          offsets.first
        end
      end
    end
  end
end