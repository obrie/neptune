require 'neptune/batch'
require 'neptune/broker_collection'
require 'neptune/config'
require 'neptune/helpers/assertions'
require 'neptune/helpers/loggable'
require 'neptune/topic_collection'
require 'neptune/api'

module Neptune
  # A group of brokers
  class Cluster
    include Helpers::Assertions
    include Helpers::Loggable

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

    # Looks up the topic with the given name.  Missing topic metadata will be
    # looked up in the cluster.
    # @return [Neptune::Topic] the topic `nil` if the topic is unknown
    def topic(name)
      if !@topics[name]
        refresh!([name])
      elsif refresh?
        # On periodic refreshes, errors shouldn't prevent the client from
        # continuing to interact with Kafka
        refresh([name])
      end

      @topics[name]
    end

    # Looks up the topic with the given name.
    # @raise [Neptune::InvalidTopicError] if the topic is not found
    # @return [Neptune::Topic]
    def topic!(name)
      topic(name) || raise(InvalidTopicError.new("Unknown topic: #{name.inspect}"))
    end

    # Whether a refresh of the cluster metadata is needed
    # @return [Boolean]
    def refresh?
      !last_refreshed_at || (Time.now - last_refreshed_at) * 1000 >= config[:refresh_interval]
    end

    # Refreshes the metadata associated with the given topics or raise an exception
    # if it fails
    # @return [Boolean]
    def refresh!(topic_names)
      # Add already-known topics
      topic_names += topics.map(&:name)
      topic_names.uniq!

      # Shuffle so that every client doesn't attempt to refresh on the same broker
      brokers = self.brokers.to_a.shuffle

      # Attempt a refresh on the first available broker
      retriable(:metadata, attempts: brokers.count, backoff: 0) do |index|
        metadata = brokers[index].metadata(topic_names)
        metadata.topics.each {|topic| topics << topic}
        metadata.brokers.each {|broker| self.brokers << broker}

        @last_refreshed_at = Time.now
        true
      end
    end

    # Refreshes the metadata associated with the given topics
    # @return [Boolean]
    def refresh(topic_names)
      refresh!(topic_names)
    rescue APIError
      false
    end

    # Forces a refresh the next time metadata attempts to be accessed for a
    # topic
    def reset_refresh
      @last_refreshed_at = nil
    end

    # Publish a value to a given topic
    # @return [Boolean]
    def produce(topic_name, value, options = {}, &callback)
      assert_valid_keys(options, [:key])

      run_or_update_batch(:produce,
        Api::Produce::Request.new(
          topic_name: topic_name,
          messages: [Message.new(key: options[:key], value: value)]
        ),
        callback
      )
    end

    # Publish a value to a given topic or raise an exception if it fails
    # @return [Boolean]
    def produce!(*args, &callback)
      produce(*args, &callback).tap do |responses|
        raise(APIError.new(responses.error_code)) if responses && !responses.success?
      end
    end

    # Fetch messages from the given topic / partition
    # @return [Array<Neptune::Message>]
    def fetch(topic_name, partition_id, offset, &callback)
      run_or_update_batch(:fetch,
        Api::Fetch::Request.new(
          topic_name: topic_name,
          partition_id: partition_id,
          offset: offset,
          max_bytes: config[:max_fetch_bytes]
        ),
        callback
      )
    end

    # Fetch messages from the given topic / partition or raise an exception if it fails
    # @return [Array<Neptune::Message>]
    def fetch!(*args, &callback)
      fetch(*args, &callback).tap do |responses|
        raise(APIError.new(responses.error_code)) if responses && !responses.success?
      end
    end

    # Runs a batch of API calls
    # @yield [Neptune::Batch]
    # @return [Neptune::Batch]
    def batch(api_name)
      if batch = @batches[api_name]
        raise(Error.new("A batch has already been started for #{api_name} API"))
      else
        batch = @batches[api_name] = Api.get(api_name)::Batch.new(self)
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
    def run_or_update_batch(api_name, request, callback)
      if batch = @batches[api_name]
        batch.add(request, callback)
        nil
      else
        batch = Api.get(api_name)::Batch.new(self)
        batch.add(request, callback)
        batch.run
      end
    end
  end
end