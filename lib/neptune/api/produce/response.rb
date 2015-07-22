require 'neptune/resource'

module Neptune
  module Api
    module Produce
      class Response < Resource
        # The topic this result corresponds to
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition this result corresponds to
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The error from this partition
        # @return [Neptune::ErrorCode]
        attribute :error_code, ErrorCode

        # The offset assigned to the first message in the message set appended to
        # this partition
        # @return [Fixnum]
        attribute :offset, Int64

        # Whether the produce was successful in this partition
        # @return [Boolean]
        def success?
          error_code.success?
        end

        # Whether the error, if any, is retrable in this partition
        # @return [Boolean]
        def retriable?
          error_code.retriable?
        end
      end
    end
  end
end