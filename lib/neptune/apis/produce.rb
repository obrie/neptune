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
    # @return [Fixnum]
    attribute :error_code, Int16

    # The offset assigned to the first message in the message set appended to
    # this partition
    # @return [Fixnum]
    attribute :offset, Int64

    # The name of the error associated with the current error code
    # @return [Symbol]
    def error_name
      ERROR_CODES[error_code]
    end

    # Whether the produce was successful in this partition
    # @return [Boolean]
    def success?
      error_name == :no_error
    end

    # Whether the error, if any, is retrable in this partition
    # @return [Boolean]
    def retriable?
      [:leader_not_available, :not_leader_for_partition].include?(error_name)
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

    # Look up the response for the given partition message
    # @return [Neptune::PartitionResponse]
    def response_for(partition_message)
      partition_responses.detect {|response| response.partition_id == partition_message.partition_id}
    end

    # Identify messages which have failed
    # @return [Array<Neptune::PartitionMessage>]
    def failed_messages(partition_messages)
      partition_messages.select {|message| !response_for(message).success?}
    end

    # Identify messages which can be retried
    # @return [Array<Neptune::PartitionMessage>]
    def retriable_messages(partition_messages)
      partition_messages.select {|message| response_for(message).retriable?}
    end
  end

  class ProduceResponse < Resource
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
    def error_name
      failed_response = topic_responses.map(&:partition_responses).flatten.reject(&:success?).first
      if failed_response
        failed_response.error_name
      end
    end

    # Look up the response for the given topic message
    # @return [Neptune::TopicResponse]
    def response_for(topic_message)
      topic_responses.detect {|response| response.topic_name == topic_message.topic_name}
    end

    # Identify messages which have failed
    # @return [Array<Neptune::PartitionMessage>]
    def failed_messages(topic_messages)
      topic_messages.inject([]) do |failed, topic_message|
        partition_messages = response_for(topic_message).failed_messages(topic_message.partition_messages)
        failed << TopicMessage.new(:topic_name => topic_message.topic_name, :partition_messages => partition_messages) if partition_messages.any?
        failed
      end
    end

    # Identify messages which can be retried
    # @return [Array<Neptune::PartitionMessage>]
    def retriable_messages(topic_messages)
      topic_messages.inject([]) do |retriable, topic_message|
        partition_messages = response_for(topic_message).retriable_messages(topic_message.partition_messages)
        retriable << TopicMessage.new(:topic_name => topic_message.topic_name, :partition_messages => partition_messages) if partition_messages.any?
        retriable
      end
    end
  end
end