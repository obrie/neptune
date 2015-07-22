require 'set'
require 'neptune/api/produce/request'
require 'neptune/message'

module Neptune
  # Tracks a batch of messages to produce
  class Batch
    # The last error code that was encountered when publishing
    # @return [Neptune::ErrorCode]
    attr_reader :last_error_code

    # The list of messages that failed to be published
    # @return [Array<Hash>]
    attr_reader :failed_messages

    def initialize(cluster, &block) #:nodoc:
      @cluster = cluster
      @messages = []
      @failed_messages = []

      if block
        instance_eval(&block)
        process
      end
    end

    # Adds the give message to this batch
    # @return [Boolean] true, always
    def add(topic_name, value, key = nil)
      @messages << {topic_name: topic_name, value: value, key: key}
      true
    end

    # Processes all of the messages in the batch
    # @return [Boolean] true if all messages are sent successfully, otherwise false
    def process
      # Track remaining messages to be processed
      remaining = Set.new(@messages.map {|m| m.merge(id: m.object_id)})

      @cluster.retriable('Produce') do
        by_leader = messages_by_leader(remaining)
        by_leader.each do |leader, by_topic|
          # Create requests
          requests = build_requests(by_topic)

          # Send the request
          responses = leader.produce(requests)

          if responses.success?
            # Remove all messages that were just published
            messages = by_topic.values.map(&:values).flatten
            remaining.subtract(messages)
          else
            # Remove individual messages that were successful or failed
            responses.each do |response|
              messages = by_topic[response.topic_name][response.partition_id]

              # Track successful / failed messages
              if response.success? || !response.retriable?
                remaining.subtract(messages)
                failed_messages.concat(messages) unless response.retriable?
              end
              @last_error_code = response.error_code unless response.success?
            end
          end
        end

        # Stop retrying when there are no more messages remaining
        remaining.empty?
      end

      # Any remaining unprocessed messages are marked as failed
      failed_messages.concat(remaining.to_a)
      failed_messages.each {|message| message.delete(:id)}

      success?
    end

    # Whether this batch was processed successfully
    # @return [Boolean]
    def success?
      failed_messages.empty?
    end

    private
    # Map a set of messages by leader -> topic -> partition -> messages
    # @return [Hash]
    def messages_by_leader(messages)
      by_leader = Hash.new {|h, k| h[k] = Hash.new {|h, k| h[k] = Hash.new {|h, k| h[k] = []}}}

      messages.each do |message|
        topic = @cluster.topic!(message[:topic_name])
        partition = topic.partition_for(message[:key])
        if partition
          by_leader[partition.leader][topic][partition] << message
        end
      end

      by_leader
    end

    # Builds a set of requests based on a topic and set of messages grouped by
    # partition
    # @return [Array<Neptune::Api::Produce::Request>]
    def build_requests(by_topic)
      requests = []

      by_topic.each do |topic, by_partition|
        by_partition.each do |partition, messages|
          request = Api::Produce::Request.new(
            topic_name: topic.name,
            partition_id: partition.id,
            messages: messages.map do |message|
              Message.new(key: message[:key], value: message[:value])
            end
          )
          request.compress(topic.compression_codec) if topic.compressed?
          requests << request
        end
      end

      requests
    end
  end
end