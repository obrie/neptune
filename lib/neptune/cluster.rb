require 'neptune/batch'
require 'neptune/broker'
require 'neptune/config'
require 'neptune/loggable'
require 'neptune/topic'

module Neptune
  # A group of brokers
  class Cluster
    include Loggable

    # List of brokers known to the cluster
    # @return [Hash<String, Neptune::Broker>]
    attr_reader :brokers

    # Known topics in the cluster
    # @return [Hash<String, Neptune::Topic>]
    attr_reader :topics

    # The pool of open connections
    # @return [Array<Neptune::Connection>]
    attr_reader :connections

    # The time at which the metadata for this cluster was last refreshed
    # @return [Time]
    attr_reader :last_refreshed_at

    # The configuration for client interaction with brokers
    # @return [Neptune::Config]
    attr_reader :config

    # Creates a new Kafka cluster with the given seed brokers.
    def initialize(brokers = ['localhost:9092'], config = {})
      @brokers = {}
      @topics = {}
      @config = Config.new(config)
      @connections = Hash.new do |connections, uri|
        host, port = uri.split(':')
        connections[uri] = Connection.new(host, port, @config)
      end

      # Add seed brokers
      brokers.each do |uri|
        self.brokers[uri] = Broker.new(:uri => uri, :cluster => self)
      end
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
      if !@topics.key?(name)
        refresh([name])
      elsif refresh?
        # On periodic refreshes, errors shouldn't prevent the client from
        # continuing to interact with Kafka
        refresh([name], :raise_on_error => false)
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
      !@last_refreshed_at || (Time.now - @last_refreshed_at) * 1000 >= config[:metadata_refresh_interval]
    end

    # Refreshes the metadata associated with the given topics
    # @return [Boolean]
    def refresh(topic_names, options = {})
      options = {:raise_on_error => true}.merge(options)

      # Add already-known topics
      topic_names += topics.map(&:name)
      topic_names.uniq!

      # Attempt a refresh on the first available broker
      index = 0
      begin
        metadata = brokers.values[index].metadata(topic_names)

        # Update topics
        metadata.topics.each do |topic|
          topic.cluster = self
          topics[topic.name] = topic
        end

        # Update brokers
        metadata.brokers.each do |broker|
          # Index by ID and remove any indexes by URI since the ID is now known
          broker.cluster = self
          brokers[broker.id] = broker
          brokers.delete(broker.uri)
        end

        @last_refreshed_at = Time.now
        true
      rescue ConnectionError => ex
        logger.warn "[Neptune] Failed to retrieve metadata: #{ex.message}"
        if index == brokers.count - 1
          # No more brokers left to try on: raise
          raise if options[:raise_on_error]
        else
          # Try on the next broker
          index += 1
          retry
        end
      end
    end

    # Forces a refresh the next time metadata attempts to be accessed for a
    # topic
    def reset_refresh
      @last_refreshed_at = nil
    end

    # Send a value to a given topic
    # @param [Hash] options The produce options
    # @option options [Fixnum] :ack_timeout (100) The total number of playlists to get
    # @option options [Fixnum] :required_acks (0) The number of playlists to skip when loading the list
    def produce!(topic_name, value, key = nil)
      if @batch
        @batch.add(topic_name, value, key)
      else
        batch = Batch.new(self) { add(topic_name, value, key) }
        batch.success? || raise(APIError.new(batch.last_error_code))
      end
    end

    # Send a value to a given topic
    # @param [Hash] options The produce options
    # @option options [Fixnum] :ack_timeout (100) The total number of playlists to get
    # @option options [Fixnum] :required_acks (0) The number of playlists to skip when loading the list
    def produce(*args)
      produce!(*args)
    rescue APIError
      false
    end

    # Produces a batch of messages
    # @yield [Neptune::Batch]
    # @return [Neptune::Batch]
    def batch(&block)
      if @batch
        yield
      else
        batch = Batch.new(self)
        begin
          @batch = batch
          yield
          @batch.process
          batch
        ensure
          @batch = nil
        end
      end
    end

    # Closes all open connections to brokers
    # @return [Boolean] always true
    def shutdown
      connections.each {|connection| connection.close}
      true
    end

    # Attempts a block the given number of times.  This will catch allow retry
    # on certain exceptions as well.
    # 
    # To halt events, simply `throw :halt` with the error code that's causing
    # the problem.
    # 
    # @return [Boolean] whether the block completed successfully
    def retriable(attempts, exceptions = [ConnectionError])
      completed = false

      catch(:halt) do
        attempts.times do |attempt|
          begin
            break if completed = yield
          rescue *exceptions => ex
            logger.warn "[Neptune] Failed to call Kafka API on attempt ##{attempt}: #{ex.message}"
            raise if attempt == attempts - 1
          end

          if attempt < attempts - 1
            # Force a refresh and backoff a little bit
            reset_refresh
            sleep(config[:retry_backoff] / 1000.0)
          end
        end
      end

      completed
    end
  end
end