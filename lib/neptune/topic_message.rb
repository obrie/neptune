require 'neptune/resource'
require 'neptune/partition_message'

module Neptune
  # A set of messages for a given topic
  class TopicMessage < Resource
    # The name of the topic this message belongs to
    # @return [String]
    attribute :topic_name, String

    # Messages by partition
    # @return [Array<Neptune::PartitionMessage>]
    attribute :partition_messages, ArrayOf[PartitionMessage]

    def initialize(*) #:nodoc:
      super
      @partition_messages ||= []
    end
  end
end
