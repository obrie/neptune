require 'neptune/topic_message'

module Neptune
  class ProduceRequest < Request
    # How many acknowledgements the servers should receive before responding to
    # the request
    # @return [Fixnum]
    attribute :required_acks, Int16

    # Maximum time, in milliseconds, the server can await the receipt of the
    # number of required acknowledgements
    # @return [Fixnum]
    attribute :ack_timeout, Int32

    # The messages to send
    # @return [Array<Neptune::TopicMessage>]
    attribute :topic_messages, ArrayOf[TopicMessage]

    def initialize(*) #:nodoc:
      super
      self.api_key = 0
    end
  end

  class ProducePartitionResponse < Resource
    # The partition this response corresponds to
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
      error_code == :no_error
    end

    # Whether the error, if any, is retrable in this partition
    # @return [Boolean]
    def retriable?
      error_code.is?(:leader_not_available, :not_leader_for_partition)
    end
  end

  class ProduceTopicResponse < Resource
    # The topic messages are being published to
    # @return [String]
    attribute :topic_name, String

    # Responses for each partition within the topic
    # @return [Array<ProducePartitionResponse>]
    attribute :partition_responses, ArrayOf[ProducePartitionResponse]

    # Whether all partitions were produced to successfully
    # @return [Boolean]
    def success?
      partition_responses.all? {|response| response.success?}
    end
  end

  class ProduceResponse < Response
    # Responses for each topic within the cluster
    # @return [Array<ProduceTopicResponse>]
    attribute :topic_responses, ArrayOf[ProduceTopicResponse]

    def initialize(*) #:nodoc:
      super
      @topic_responses ||= []
    end

    # Whether all messages were successfully produced
    # @return [Boolean]
    def success?
      topic_responses.all? {|response| response.success?}
    end

    # The first available error in the response
    # @return [Fixnum]
    def error_code
      failed_response = topic_responses.map(&:partition_responses).flatten.reject(&:success?).first
      if failed_response
        failed_response.error_code
      end
    end
  end
end