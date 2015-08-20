require 'neptune/batch'

module Neptune
  module Api
    module Fetch
      # A batch of one or more Fetch requests
      class Batch < Neptune::Batch
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