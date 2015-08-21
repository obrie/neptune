require 'neptune/resource'

module Neptune
  module Api
    module Offset
      class Request < Resource
        # The topic to request
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition to request
        # @return [Fixnum]
        attribute :partition_id, Int32

        # Used to ask for all messages before a certain time (ms). There are two
        # special values. Specify -1 to receive the latest offset (i.e. the
        # offset of the next coming message) and -2 to receive the earliest
        # available offset.
        # @return [Fixnum]
        attribute :time, Int64

        # The maximum number of offsets to return.  Always 1.
        # @return [Fixnum]
        attribute :max_offsets, Int32

        def initialize(*)
          super
          self.max_offsets = 1
        end

        def time=(value) #:nodoc:
          if value == :latest
            value = -1
          elsif value == :earliest
            value = -2
          end

          @time = value
        end
      end
    end
  end
end