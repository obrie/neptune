require 'forwardable'
require 'neptune/buffer'
require 'neptune/errors'
require 'neptune/support/socket'
require 'neptune/types/int32'

module Neptune
  # A socket connection to a node in the cluster
  class Connection
    extend Forwardable

    delegate [:host, :port, :config, :config=, :close] => :@socket

    def initialize(host, port, config = {}) #:nodoc:
      @socket = Support::Socket.new(host, port, config)
    end

    # Verifies that the connection is alive.  If it isn't, this will automatically
    # reconnect.
    # @raise [ConnectionError] if the socket cannot connect or has timed out
    # @return [Boolean] true, always
    def verify
      catch_errors do
        @socket.reconnect unless @socket.alive?
        true
      end
    end

    # Reads the next message from the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Neptune::Buffer]
    def read(correlation_id)
      catch_errors do
        # Determine the time by which a response must be read
        deadline = Time.now + (config[:read_timeout] / 1000.0)

        buffer = nil
        until buffer
          ms_left = (deadline - Time.now).to_f * 1000

          if ms_left > 0 || @socket.ready_for_read?
            # Attempt to read the next buffer
            next_buffer, next_correlation_id = read_buffer(ms_left)
            if correlation_id == next_correlation_id
              buffer = next_buffer
            end
          else
            # Time has run out
            raise Errno::ETIMEDOUT.new('exceeded read timeout')
          end
        end

        buffer
      end
    end

    # Writes the given data to the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Boolean] true, always
    def write(data)
      catch_errors { @socket.write(data) }
    end

    private
    # Reads the next message buffer from the socket
    # @return [Neptune::Buffer, Fixnum]
    def read_buffer(timeout)
      length = @socket.read(4, timeout: timeout).unpack('N').first
      buffer = Buffer.new(@socket.read(length, timeout: timeout))
      correlation_id = Types::Int32.from_kafka(buffer)
      [buffer.rewind, correlation_id]
    end

    # Runs the given block, catching any socket errors and raising them as
    # a ConnectionError
    def catch_errors
      yield
    rescue SystemCallError, IOError, OpenSSL::SSL::SSLError => ex
      @socket.close
      raise ConnectionError.new("#{ex.class}: #{ex.message}", ex)
    end
  end
end