require 'neptune/message'
require 'neptune/resource'

module Neptune
  class PartitionMessage < Resource
    # The partition this message belongs to
    # @return [Fixnum]
    attribute :partition_id, Int32

    # The actual collection of messages
    # @return [Array<Neptune::Message>]
    attribute :messages, [Message]
  end
end