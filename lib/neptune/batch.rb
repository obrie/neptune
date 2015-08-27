require 'forwardable'
require 'set'
require 'neptune/error_code'
require 'neptune/helpers/pretty_print'

module Neptune
  # Tracks a batch of api requests
  class Batch
    include Helpers::PrettyPrint
    extend Forwardable

    # The base module for this batch's API
    # @return [Module]
    def self.api
      @api ||= Api.for_class(self)
    end

    # The name for this batch's API
    # @return [String]
    def self.api_name
      @api_name ||= Api.name_for(api)
    end

    # The list of requests to process
    # @return [Array<Neptune::Resource>]
    attr_reader :requests

    # The responses for each request, keyed by request
    # @return [Hash<Neptune::Resource, Neptune::Resource>]
    attr_reader :responses_by_request

    def initialize(cluster, options = {}, &block) #:nodoc:
      @cluster = cluster
      @options = options
      @requests = []
      @responses_by_request = {}
      @callbacks = {}
    end

    # The responses from all of the requests in this batch, including those
    # that failed
    # @return [Neptune::Resource]
    def responses
      api::BatchResponse.new(responses: responses_by_request.values.uniq)
    end

    # The response for the given request
    # @return [Netpune::Resource]
    def response_for(request)
      @responses_by_request[request]
    end

    # The list of requests that failed to be delivered successfully
    # @return [Array<Neptune::Resource>]
    def failed_requests
      requests.select do |request|
        response = response_for(request)
        !response || !response.success?
      end
    end

    # The list of requests that failed to be delivered successfully, but are retriable
    # @return [Array<Neptune::Resource>]
    def retriable_requests
      requests.select do |request|
        response = response_for(request)
        !response || response.retriable?
      end
    end

    # Adds the give request to this batch
    # @return [Boolean] true, always
    def add(request, callback = nil)
      @requests << request
      @callbacks[request] = callback if callback
      true
    end

    # Processes all of the requests in the batch
    # @return [Neptune::Resource] The BatchResponse object for this batch's API
    def run
      @cluster.retriable(api_name) do
        process(retriable_requests)

        # Stop retrying when there are no more requests to retry
        retriable_requests.empty?
      end

      responses
    end

    protected
    # Process requests, grouped by broker
    def process(requests)
      by_broker = requests.group_by {|request| broker_for(request)}
      by_broker.each do |broker, requests|
        process_broker(broker, requests)
      end
    end

    # Process requests for the given broker
    # @return [Neptune::Resource]
    def process_broker(broker, requests)
      if broker
        # Send the requests to the broker
        responses = broker.send(api_name, requests, @options).responses
      else
        # Create responses based on the lack of a broker
        responses = requests.map do |request|
          api::Response.new(
            topic_name: request.topic_name,
            partition_id: request.partition_id,
            error_code: ErrorCode[:leader_not_available]
          )
        end
      end

      # Record responses
      requests.each do |request|
        response = responses.detect {|response| response.topic_name == request.topic_name && response.partition_id == request.partition_id}
        record_response(request, response)
      end

      # Run callbacks
      requests.each {|request| run_callbacks(request)}

      responses
    end

    # Associates a response with the given request
    def record_response(request, response)
      @responses_by_request[request] = response
    end

    # Runs any callbacks registered with the given request
    def run_callbacks(request)
      callback = @callbacks[request]
      response = response_for(request)
      if callback && response && response.success?
        callback.call(api::BatchResponse.new(responses: [response]))
      end
    end

    private
    delegate [:api, :api_name] => :'self.class'

    # The broker that the given request should be delivered to
    # @return [Neptune::Broker]
    def broker_for(request)
      topic = @cluster.topic!(request.topic_name)
      partition = topic.partition!(request.partition_id)
      partition && partition.leader
    end

    def pretty_print_ignore #:nodoc:
      [:@cluster, :@callbacks]
    end
  end
end