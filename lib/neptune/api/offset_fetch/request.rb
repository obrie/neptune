require 'neptune/resource'

module Neptune
  module Api
    module OffsetFetch
      class Request < Resource
        # The topic to request
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition to request
        # @return [Fixnum]
        attribute :partition_id, Int32
      end
    end
  end
end