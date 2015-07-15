require 'neptune/message'
require 'neptune/resource'

module Neptune
  # A set of messages for a given partition
  class PartitionMessage < Resource
    # The partition this message belongs to
    # @return [Fixnum]
    attribute :partition_id, Int32

    # The actual collection of messages
    # @return [Array<Neptune::Message>]
    attribute :messages, SizeBoundArrayOf[Message]

    def initialize(*) #:nodoc:
      super
      @messages ||= []
    end
  end
end