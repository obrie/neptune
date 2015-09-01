require 'neptune/batch'

module Neptune
  module Api
    module Fetch
      # A batch of one or more Fetch requests
      class Batch < Neptune::Batch
        # Fetch messages from the given topic / partition
        # @return [Boolean] true, always
        def fetch(topic_name, partition_id, offset, options = {}, &callback)
          assert_valid_keys(options, :max_bytes)

          add(Api::Fetch::Request.new(
            topic_name: topic_name,
            partition_id: partition_id,
            offset: offset,
            max_bytes: options[:max_bytes] || @cluster.config.max_fetch_bytes
          ), &callback)
        end

        protected
        # Processes requests for the given broker.  After being processed,
        # the highwater mark offset will be updated in the cluster partitions.
        def process_broker(broker, requests)
          super.tap do |responses|
            responses.each do |response|
              # Track highwater mark offset
              topic = @cluster.topic!(response.topic_name)
              partition = topic.partition!(response.partition_id)
              partition.highwater_mark_offset = response.highwater_mark_offset

              # Provide access to the partition from the message
              response.messages.each {|message| message.partition = partition}
            end
          end
        end
      end
    end
  end
end