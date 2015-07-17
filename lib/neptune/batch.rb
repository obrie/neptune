require 'set'
require 'neptune/message'
require 'neptune/partition_message'
require 'neptune/topic_message'

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
          # Create topic messages
          topic_messages = by_topic.map {|topic, by_partition| build_topic_message(topic, by_partition)}

          # Send the request
          response = leader.produce(topic_messages)

          if response.success?
            # Remove all messages that were just published
            messages = by_topic.values.map(&:values).flatten
            remaining.subtract(messages)
          else
            # Remove individual messages that were successful or failed
            response.topic_responses.each do |topic_response|
              topic_response.partition_responses.each do |partition_response|
                messages = by_topic[topic_response.topic_name][partition_response.partition_id]

                # Track successful / failed messages
                if partition_response.success? || !partition_response.retriable?
                  remaining.subtract(messages)
                  failed_messages.concat(messages) unless partition_response.retriable?
                end
                @last_error_code = partition_response.error_code unless partition_response.success?
              end
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

    # Builds a +TopicMessage+ based on a topic and set of messages grouped by partition
    # @return [Array<Neptune::TopicMessage]
    def build_topic_message(topic, by_partition)
      TopicMessage.new(
        topic_name: topic.name,
        partition_messages: by_partition.map do |partition, messages|
          partition_message = PartitionMessage.new(
            partition_id: partition.id,
            messages: messages.map do |message|
              Message.new(key: message[:key], value: message[:value])
            end
          )
          partition_message.compress(topic.compression_codec) if topic.compressed?
          partition_message
        end
      )
    end
  end
end