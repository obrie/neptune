require 'neptune/resource'

module Neptune
  module Api
    module OffsetCommit
      class Request < Resource
        # The topic to request
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition to request
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The value to set the latest offset to for the consumer
        # @return [Fixnum]
        attribute :offset, Int64

        # The time to associate with the offset
        # @return [Fixnum]
        attribute :timestamp, Int64, version: 1..1

        # Arbitrary data to associate with the offset. This could be a filename,
        # small piece of state, etc. It will be passed back to the client when
        # the offset is fetched.
        # @return [String]
        attribute :metadata, String

        def initialize(*) #:nodoc:
          super
          self.timestamp ||= Time.now.to_i
        end
      end
    end
  end
end