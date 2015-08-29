require 'neptune/batch'
require 'neptune/broker_collection'
require 'neptune/config'
require 'neptune/support/loggable'
require 'neptune/topic_collection'
require 'neptune/api'

module Neptune
  # A group of brokers
  class Cluster
    include Support::Loggable

    # List of brokers known to the cluster
    # @return [Neptune::BrokerCollection]
    attr_reader :brokers

    # Known topics in the cluster
    # @return [Neptune::TopicCollection]
    attr_reader :topics

    # The time at which the metadata for this cluster was last refreshed
    # @return [Time]
    attr_reader :last_refreshed_at

    # The configuration for client interaction with brokers
    # @return [Neptune::Config]
    attr_reader :config

    # Creates a new Kafka cluster with the given seed brokers.
    def initialize(brokers = ['localhost:9092'], config = {})
      @brokers = BrokerCollection.new(self)
      @topics = TopicCollection.new(self)
      @config = Config.new(config)
      @batches = {}

      # Add seed brokers
      brokers.each {|uri| @brokers.create(uri: uri)}
    end

    # Looks up the broker with the given unique id
    # @return [Neptune::Broker]
    def broker(id)
      @brokers[id]
    end

    # Looks up the topic with the given name or raises an exception if the topic
    # is not found
    # @return [Neptune::Topic]
    def topic!(name)
      if !@topics[name] || refresh?
        refresh!([name])
      end

      @topics[name] || ErrorCode[:unknown_topic_or_partition].raise
    end

    # Looks up the topic with the given name.  Missing topic metadata will be
    # looked up in the cluster.
    # @return [Neptune::Topic] the topic `nil` if the topic is unknown
    def topic(name)
      topic!(name)
    rescue Error
      nil
    end

    # Looks up all topics that are known in the cluster
    # @return [Array<Neptune::Topic>]
    def known_topics
      known_topics!
    rescue Error
      []
    end

    # Looks up all topics that are known in the cluster or raises an exception
    # if the lookup fails
    # @return [Array<Neptune::Topic>]
    def known_topics!
      refresh!
      self.topics.to_a
    end

    # Whether a refresh of the cluster metadata is needed
    # @return [Boolean]
    def refresh?
      !last_refreshed_at || (Time.now - last_refreshed_at) * 1000 >= config[:refresh_interval]
    end

    # Refreshes the metadata associated with the given topics or raise an exception
    # if it fails
    # @return [Boolean]
    def refresh!(topic_names = [])
      # Add already-known topics
      topic_names += topics.map(&:name)
      topic_names.uniq!

      # Shuffle so that every client doesn't attempt to refresh on the same broker
      brokers = self.brokers.to_a.shuffle

      # Attempt a refresh on the first available broker
      retriable(:metadata, attempts: [brokers.count, config[:retry_count]].max, backoff: 0) do |index|
        metadata = brokers[index % brokers.count].metadata(topic_names)
        metadata.topics.each {|topic| topics << topic if topic.exists?}
        metadata.brokers.each {|broker| self.brokers << broker}

        @last_refreshed_at = Time.now
        true
      end
    end

    # Refreshes the metadata associated with the given topics
    # @return [Boolean]
    def refresh(topic_names)
      refresh!(topic_names)
    rescue Error
      false
    end

    # Forces a refresh the next time metadata attempts to be accessed for a
    # topic
    def reset_refresh
      @last_refreshed_at = nil
    end

    # Publish a value to a given topic or raise an exception if it fails
    # @return [Neptune::Produce::Response]
    def produce!(topic_name, value, options = {}, &callback)
      options = options.dup

      run_or_update_batch(:produce,
        Api::Produce::Request.new(
          topic_name: topic_name,
          messages: [Message.new(key: options.delete(:key), value: value)]
        ),
        callback, options
      )
    end

    # Publish a value to a given topic
    # @return [Neptune::Produce::Response]
    def produce(topic_name, value, options = {}, &callback)
      catch_errors(Api::Produce::Response) do
        produce!(topic_name, value, options, &callback)
      end
    end

    # Fetch messages from the given topic / partition or raise an exception if it fails
    # @return [Neptune::Fetch::Response]
    def fetch!(topic_name, partition_id, offset, options = {}, &callback)
      run_or_update_batch(:fetch,
        Api::Fetch::Request.new(
          topic_name: topic_name,
          partition_id: partition_id,
          offset: offset,
          max_bytes: options.delete(:max_bytes) || config.max_fetch_bytes
        ),
        callback, options
      )
    end

    # Fetch messages from the given topic / partition
    # @return [Neptune::Fetch::Response]
    def fetch(topic_name, partition_id, offset, options = {}, &callback)
      catch_errors(Api::Fetch::Response) do
        fetch!(topic_name, partition_id, offset, options, &callback)
      end
    end

    # Looks up valid offsets available within a given topic / partition or
    # raises an exception if the request fails
    # @return [Neptune::Offset::Response]
    def offset!(topic_name, partition_id, options = {}, &callback)
      options = {time: :latest}.merge(options)

      run_or_update_batch(:offset,
        Api::Offset::Request.new(
          topic_name: topic_name,
          partition_id: partition_id,
          time: options.delete(:time)
        ),
        callback, options
      )
    end

    # Looks up valid offsets available within a given topic / partition
    # @return [Neptune::Offset::Response]
    def offset(topic_name, partition_id, options = {}, &callback)
      catch_errors(Api::Offset::Response) do
        offset!(topic_name, partition_id, options, &callback)
      end
    end

    # Looks up the metadata associated with a consumer group or raise an
    # exception if the metadata is not found
    # @return [Neptune::ConsumerMetadata::Response]
    def consumer_metadata!(options = {})
      brokers = self.brokers.to_a.shuffle

      retriable(:consumer_metadata, attempts: [brokers.count, config[:retry_count]].max, backoff: 0) do |index|
        metadata = brokers[index % brokers.count].consumer_metadata(options)

        if metadata.success?
          self.brokers << metadata.coordinator
          metadata
        else
          metadata.error_code.raise
        end
      end
    end

    # Looks up the metadata associated with a consumer group
    # @return [Neptune::ConsumerMetadata::Response]
    def consumer_metadata(options = {})
      catch_errors(Api::ConsumerMetadata::Response) do
        consumer_metadata!(options)
      end
    end

    # Looks up the broker acting as coordinator for offsets within the given
    # consumer group or raises an exception of the coordinator isn't found.
    # @return [Neptune::Broker]
    def coordinator!(options = {})
      brokers[consumer_metadata!(options).coordinator.id]
    end

    # Looks up the broker acting as coordinator for offsets within the given
    # consumer group
    # @return [Neptune::Broker]
    def coordinator(options = {})
      coordinator!(options)
    rescue Error
      nil
    end

    # Looks up the latest offset for a consumer in the given topic / partition
    # or raises an exception if the request fails
    # @return [Neptune::OffsetFetch::Response]
    def offset_fetch!(topic_name, partition_id, options = {}, &callback)
      run_or_update_batch(:offset_fetch,
        Api::OffsetFetch::Request.new(
          topic_name: topic_name,
          partition_id: partition_id
        ),
        callback, options
      )
    end

    # Looks up the latest offset for a consumer in the given topic / partition
    # @return [Neptune::OffsetFetch::Response]
    def offset_fetch(topic_name, partition_id, options = {}, &callback)
      catch_errors(Api::OffsetFetch::Response) do
        offset_fetch!(topic_name, partition_id, options, &callback)
      end
    end


    # Tracks the latest offset for a consumer in the given topic / partition
    # or raises an exception if the request fails
    # @return [Neptune::OffsetCommit::Response]
    def offset_commit!(topic_name, partition_id, offset, options = {}, &callback)
      run_or_update_batch(:offset_commit,
        Api::OffsetCommit::Request.new(
          topic_name: topic_name,
          partition_id: partition_id,
          offset: offset,
          metadata: options.delete(:metadata)
        ),
        callback, options
      )
    end

    # Tracks the latest offset for a consumer in the given topic / partition
    # @return [Neptune::OffsetCommit::Response]
    def offset_commit(topic_name, partition_id, offset, options = {}, &callback)
      catch_errors(Api::OffsetCommit::Response) do
        offset_commit!(topic_name, partition_id, offset, options, &callback)
      end
    end

    # Runs a batch of API calls
    # @yield [Neptune::Batch]
    # @return [Neptune::Batch]
    def batch(api_name, options = {})
      if batch = @batches[api_name]
        raise ArgumentError, "A batch has already been started for #{api_name} API"
      else
        batch = @batches[api_name] = Api.get(api_name)::Batch.new(self, options)
        begin
          yield
          batch.run
          batch
        ensure
          @batches.delete(api_name)
        end
      end
    end

    # Closes all open connections in brokers
    # @return [Boolean] always true
    def shutdown
      brokers.each {|broker| broker.close}
      true
    end

    # Attempts a block the given number of times.  This will catch allow retry
    # on certain exceptions as well.
    # 
    # To halt events, simply `throw :halt` with the error code that's causing
    # the problem.
    # 
    # @return [Boolean] whether the block completed successfully
    def retriable(api_name, options = {})
      attempts = options.fetch(:attempts, config[:retry_count])
      exceptions = options.fetch(:exceptions, [ConnectionError])
      backoff = options.fetch(:backoff, config[:retry_backoff])
      completed = false

      catch(:halt) do
        attempts.times do |attempt|
          begin
            break if completed = yield(attempt)
          rescue *exceptions => ex
            logger.warn "[Neptune] Failed to call #{api_name} API: #{ex.message}"
            raise if attempt == attempts - 1
          end

          if attempt < attempts - 1
            # Force a refresh and backoff a little bit
            reset_refresh
            sleep(backoff / 1000.0) if backoff > 0
          end
        end
      end

      completed
    end

    private
    # Starts or continues a request batch for the given API.  If a callback
    # is provided, then that block will be called.  Otherwise, a single
    # request will be added to the batch.
    def run_or_update_batch(api_name, request, callback, options = {})
      if batch = @batches[api_name]
        batch.add(request, callback)
        nil
      else
        batch = Api.get(api_name)::Batch.new(self, options)
        batch.add(request, callback)
        result = batch.run
        if result.success?
          result.responses.first
        else
          result.error_code.raise
        end
      end
    end

    # Run a block, catching any errors that occur and building a response
    # object with an appropriate error code.
    # @return [Neptune::Resource]
    def catch_errors(response_class)
      yield
    rescue APIError => e
      response_class.new(error_code: e.error_code)
    rescue ConnectionError => e
      response_class.new(error_code: ErrorCode[:broker_not_available])
    rescue Error => e
      response_class.new(error_code: ErrorCode[:unknown_error])
    end
  end
end