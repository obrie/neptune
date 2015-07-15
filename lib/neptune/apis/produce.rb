require 'neptune/topic_message'

module Neptune
  class ProduceRequest < Request
    # @return [Fixnum]
    attribute :required_acks, Int16

    # @return [Fixnum]
    attribute :ack_timeout, Int32

    # @return [Array<Neptune::TopicMessage>]
    attribute :topic_messages, ArrayOf[TopicMessage]

    # @return [Array<Neptune::ProduceResponse>]
    attr_accessor :response

    def initialize(*) #:nodoc:
      super
      self.api_key = 0
    end
  end

  class ProducePartitionResponse < Resource
    # @return [Fixnum]
    attribute :partition_id, Int32

    # @return [Fixnum]
    attribute :error_code, Int16

    # @return [Fixnum]
    attribute :offset, Int64

    # Whether the produce was successful in this partition
    # @return [Boolean]
    def success?
      error_name == :no_error
    end

    # Whether the error, if any, is retrable in this partition
    # @return [Boolean]
    def retryable?
      [:leader_not_available, :not_leader_for_partition].include?(error_name)
    end
  end

  class ProduceTopicResponse < Resource
    # @return [String]
    attribute :topic_name, String

    # @return [Array<ProducePartitionResponse>]
    attribute :partition_responses, ArrayOf[ProducePartitionResponse]

    # Whether all partitions were produced to successfully
    # @return [Boolean]
    def success?
      partition_responses.all? {|response| response.success?}
    end

    # Look up the response for the given partition message
    # @return [Neptune::PartitionResponse]
    def response_for(message)
      partition_responses.detect {|response| response.partition_id == message.partition_id}
    end

    # Identify messages which have failed
    # @return [Array<Neptune::PartitionMessage>]
    def failed_messages(partition_messages)
      partition_messages.select {|message| !response_for(message).success?}
    end

    # Identify messages which can be retried
    # @return [Array<Neptune::PartitionMessage>]
    def retryable_messages(partition_messages)
      partition_messages.select {|message| response_for(message).retryable?}
    end
  end

  class ProduceResponse < Resource
    # @return [Array<ProduceTopicResponse>]
    attribute :topic_responses, ArrayOf[ProduceTopicResponse]

    # Whether all messages were successfully produced
    # @return [Boolean]
    def success?
      topic_responses.all? {|response| response.success?}
    end

    # The first available error code in the response
    # @return [Fixnum]
    def error_code
      failed_response = topic_responses.map(&:partition_responses).reject(&:success?).first
      if failed_response
        failed_response.error_code
      end
    end

    # Look up the response for the given topic message
    # @return [Neptune::TopicResponse]
    def response(message)
      topic_responses.detect {|response| response.topic_name == message.topic_name}
    end

    # Identify messages which have failed
    # @return [Array<Neptune::PartitionMessage>]
    def failed_messages(topic_messages)
      topic_messages.inject([]) do |failed, topic_message|
        partition_messages = response(topic_message).failed_messages(topic_message.partition_messages)
        failed << TopicMessage.new(:topic_name => topic_message.topic_name, :partition_messages => partition_messages) if partition_messages.any?
        failed
      end
    end

    # Identify messages which can be retried
    # @return [Array<Neptune::PartitionMessage>]
    def retryable_messages(topic_messages)
      topic_messages.inject([]) do |retryable, topic_message|
        partition_messages = response(topic_message).retryable_messages(topic_message.partition_messages)
        retryable << TopicMessage.new(:topic_name => topic_message.topic_name, :partition_messages => partition_messages) if partition_messages.any?
        retryable
      end
    end
  end
end