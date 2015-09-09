require 'forwardable'
require 'thread'
require 'neptune/buffer'
require 'neptune/errors'
require 'neptune/support/socket'
require 'neptune/support/blocking_hash'
require 'neptune/types/int32'

module Neptune
  # A socket connection to a node in the cluster
  class Connection
    extend Forwardable

    delegate [:host, :port, :config, :config=, :close, :valid?] => :@socket

    def initialize(host, port, config = {}) #:nodoc:
      @socket = Support::Socket.new(host, port, config)
      @lock = Mutex.new
      @read_thread = nil
      @readers = Queue.new
      @buffers = Support::BlockingHash.new
    end

    # Reads from the connection's socket any incoming buffers from prior
    # requests
    def read_buffers
      catch_errors do
        loop do
          # Wait for a thread to request to read
          break unless @readers.pop

          length = @socket.read(4).unpack('N').first
          buffer = Buffer.new(@socket.read(length))
          id = Types::Int32.from_kafka(buffer)
          buffer.rewind

          @buffers[id] = buffer
        end
      end
    end

    # Reads the message from the socket that matches the given correlation id.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Neptune::Buffer]
    def read(id)
      catch_errors do
        @readers << Thread.current
        @buffers[id]
      end
    end

    # Writes the given data to the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Boolean] true, always
    def write(data)
      catch_errors do
        @read_thread || @lock.synchronize do
          if @socket.status == :waiting
            @socket.connect

            # Start a separate thread for reading from the socket
            @read_thread = Thread.new { read_buffers }
          end
        end

        @socket.write(data)
      end
    end

    # Closes the any open sockets
    # @return [Boolean] true, always
    def close
      # Close the underlying socket
      @lock.synchronize { @socket.close }

      # Kill off the read thread
      if @read_thread && @read_thread != Thread.current
        begin
          @readers << nil
          @read_thread.join
        rescue ThreadError
        end
      end

      true
    end

    private
    # Runs the given block, catching any socket errors and raising them as
    # a ConnectionError
    def catch_errors
      yield
    rescue SocketError, SystemCallError, IOError, OpenSSL::SSL::SSLError => ex
      # Wake up any other threads waiting for responses
      @buffers.raise(ex)
      close

      raise ConnectionError.new("#{ex.class}: #{ex.message}", ex)
    end
  end
end