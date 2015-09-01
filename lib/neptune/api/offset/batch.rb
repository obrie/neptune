require 'neptune/batch'

module Neptune
  module Api
    module Offset
      # A batch of one or more Offset requests
      class Batch < Neptune::Batch
        # Looks up valid offsets available within a given topic / partition
        # @return [Boolean] true, always
        def offset(topic_name, partition_id, options = {}, &callback)
          assert_valid_keys(options, :time)

          add(Api::Offset::Request.new(
            topic_name: topic_name,
            partition_id: partition_id,
            time: options[:time] || :latest
          ), &callback)
        end
      end
    end
  end
end