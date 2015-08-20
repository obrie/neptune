require 'neptune/batch'

module Neptune
  module Api
    module Produce
      class Batch < Neptune::Batch
        protected
        # Process requests, grouped by broker.  Before being processed,
        # requests will be grouped by partition id and compressed.
        def process(requests)
          @mappings = {}

          # Ensure each request has a partition id
          requests.each do |request|
            topic = @cluster.topic!(request.topic_name)
            partition = topic.partition_for(request.messages.first.key)
            request.partition_id = partition.id
          end

          # Group by partition
          by_partition = requests.group_by {|request| [request.topic_name, request.partition_id]}

          # Create new requests
          requests = by_partition.map do |(topic_name, partition_id), requests|
            # Combine / compress message
            topic = @cluster.topic!(topic_name)
            request = Request.new(topic_name: topic_name, partition_id: partition_id, messages: requests.map(&:messages).flatten)
            request.compress(topic.compression_codec) if topic.compressed?

            # Map request for recording purposes
            @mappings[request] = requests

            request
          end

          super
        ensure
          @mappings = nil
        end

        # Associates a response with all of the individual requests that the
        # given request maps to
        def record_response(request, response)
          @mappings[request].each {|request| super(request, response)}
        end

        # Runs any callbacks registered with all of the individual requests that
        # the given request maps to
        def run_callbacks(request)
          @mappings[request].each {|request| super(request)}
        end

        def pretty_print_ignore #:nodoc:
          super + [:@mapings]
        end
      end
    end
  end
end