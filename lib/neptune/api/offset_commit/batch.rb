require 'neptune/batch'

module Neptune
  module Api
    module OffsetCommit
      # A batch of one or more OffsetCommit requests
      class Batch < Neptune::Batch
        # Tracks the latest offset for a consumer in the given topic / partition
        # @return [Boolean] true, always
        def offset_commit(topic_name, partition_id, offset, options = {}, &callback)
          assert_valid_keys(options, :metadata)

          add(Api::OffsetCommit::Request.new(
            topic_name: topic_name,
            partition_id: partition_id,
            offset: offset,
            metadata: options[:metadata]
          ), &callback)
        end
      end
    end
  end
end