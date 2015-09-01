require 'neptune/batch'

module Neptune
  module Api
    module OffsetFetch
      # A batch of one or more OffsetFetch requests
      class Batch < Neptune::Batch
        # Looks up the latest offset for a consumer in the given topic / partition
        # @return [Boolean] true, always
        def offset_fetch(topic_name, partition_id, options = {}, &callback)
          assert_valid_keys(options)

          add(Api::OffsetFetch::Request.new(
            topic_name: topic_name,
            partition_id: partition_id
          ), &callback)
        end
      end
    end
  end
end