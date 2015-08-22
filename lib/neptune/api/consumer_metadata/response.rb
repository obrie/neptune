require 'neptune/broker'
require 'neptune/response'

module Neptune
  module Api
    module ConsumerMetadata
      class Response < Neptune::Response
        # The error from this partition
        # @return [Neptune::ErrorCode]
        attribute :error_code, ErrorCode

        # The coordinator's unique identifier
        # @return [Fixnum]
        attribute :coordinator_id, Int32

        # The coordinator's hostname
        # @return [String]
        attribute :coordinator_host, String

        # The coordinator's port number
        # @return [Fixnum]
        attribute :coordinator_port, Int32

        delegate [:success?, :retriable?] => :error_code

        # The broker acting as coordinator
        # @return [Neptune::Broker]
        def coordinator
          Broker.new(id: coordinator_id, host: coordinator_host, port: coordinator_port)
        end
      end
    end
  end
end