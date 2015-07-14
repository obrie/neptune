require 'neptune/broker'
require 'neptune/config'
require 'neptune/topic'

module Neptune
  # A group of brokers
  class Cluster
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
      @topics = []
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

    # Looks up the topic with the given name.  If the topic metadata doesn't
    # already exist, it'll be looked up in the cluster.
    # @return [Neptune::Topic]
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

    # Whether a refresh of the cluster metadata is needed
    # @return [Boolean]
    def refresh?
      !@last_refreshed_at || (Time.now - @last_refreshed_at) * 1000 > config[:metadata_refresh_interval]
    end

    # Refreshes the metadata associated with the given topics
    # @return [Boolean]
    def refresh(topic_names, options = {})
      options = {:raise_on_error => true}.merge(options)

      # Add already-known topics
      topic_names += topics.map(&:name)
      topic_names.uniq!

      index = 0
      begin
        metadata = brokers[index].metadata(topic_names)

        # Update topics
        metadata.topics.each do |topic|
          topics[topic.name] = topic
        end

        # Update brokers
        metadata.brokers.each do |broker|
          # Index by ID and remove any indexes by URI since the ID is now known
          brokers[broker.id] = broker
          brokers.delete(broker.uri)
        end

        @last_refreshed_at = Time.now
      rescue ConnectionError => ex
        logger.warn "[Neptune] Failed to retrieve metadata: #{ex.message}"
        if index == brokers.count
          raise if options[:raise_on_error]
        else
          index += 1
          retry
        end
      end

      true
    end

    # @param [Hash] options The produce options
    # @option options [Fixnum] :ack_timeout (100) The total number of playlists to get
    # @option options [Fixnum] :required_acks (0) The number of playlists to skip when loading the list
    def produce(topic_name, value, key = nil)
      attempt(config[:retry_produce_count]) do
        topic = self.topic(topic_name)
        partition = topic.partition_for(key)

        topic_messages = [TopicMessage.new(
          :topic_name => topic_name,
          :partition_messages => [
            PartitionMessage.new(
              :partition_id => partition.id,
              :messages => [Message.new(:key => key, :value => value)]
            )
          ]
        )]

        response = partition.leader.produce(topic_messages)
        if !response.success?
          if response.retryable_messages.any?
            response.error_code
          else
            # Stop further attempts since the request can't be be retried
            throw :halt, response.error_code
          end
        end
      end

      true
    end

    # Closes all open connections to brokers
    def shutdown
      connections.each {|connection| connection.close}
    end

    private
    # Attempts a block the given number of times.  This will catch allow retry
    # on certain exceptions as well.
    # 
    # To halt events, simply `throw :halt` with the error code that's causing
    # the problem.
    def attempt(retries, errors = [ConnectionError])
      error = nil

      halt_error = catch(:halt) do
        retries.times do
          begin
            break unless error = yield
          rescue *errors => ex
            logger.warn "[Neptune] Failed to call Kafka API: #{ex.message}"

            # Force a refresh and backoff a little bit
            @last_refreshed_at = nil
            sleep(config[:retry_backoff] / 1000.0)
          end
        end
      end
      error ||= halt_error

      raise Error.new("Failed API call: #{error}") if error
    end
  end
end