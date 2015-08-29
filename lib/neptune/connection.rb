require 'socket'
require 'neptune/buffer'
require 'neptune/errors'
require 'neptune/types/int32'

module Neptune
  # A socket connection to a node in the cluster
  class Connection
    # Hostname of the node
    # @return [String]
    attr_reader :host

    # Port number of the node
    # @return [Fixnum]
    attr_reader :port

    # The current configuration for the connection
    # @return [Hash]
    attr_accessor :config

    def initialize(host, port, config = {}) #:nodoc:
      @host = host
      @port = port
      @config = {connect_timeout: 1000, read_timeout: 1000, write_timeout: 1000}.merge(config)
    end

    # Determines whether the socket is open and can be read from
    # @return [Boolean]
    def alive?
      !closed? && (!ready_for_read? || !@socket.eof?)
    rescue SystemCallError, IOError => ex
      false
    end

    # Determines whether the socket has been closed
    # @return [Boolean]
    def closed?
      !@socket || @socket.closed?
    end

    # Verifies that the connection is alive.  If it isn't, this will automatically
    # reconnect.
    # @raise [ConnectionError] if the socket cannot connect or has timed out
    # @return [Boolean] true, always
    def verify
      connect unless alive?
      true
    end

    # Waits the given amount of seconds until the connection is ready to be
    # read from
    # @return [Boolean]
    def ready_for_read?(timeout = 0)
      IO.select([@socket], nil, nil, timeout)
    end

    # Waits the given amount of seconds until the connection is ready to be
    # written to
    # @return [Boolean]
    def ready_for_write?(timeout = 0)
      IO.select(nil, [@socket], nil, timeout)
    end

    # Connects to the configured host / port
    # @raise [ConnectionError] if the socket cannot connect or has timed out
    # @return [Boolean] true, always
    def connect
      close unless closed?

      address = Socket.getaddrinfo(host, nil)
      socket_address = Socket.pack_sockaddr_in(port, address[0][3])

      @socket = Socket.new(Socket.const_get(address[0][0]), Socket::SOCK_STREAM, 0)
      begin
        @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

        begin
          @socket.connect_nonblock(socket_address)
        rescue IO::WaitWritable
          if ready_for_write?(@config[:connect_timeout])
            begin
              @socket.connect_nonblock(socket_address)
            rescue Errno::EISCONN
              # Socket is connected
            end
          else
            raise Errno::ETIMEDOUT.new("exceeded connect timeout of #{@config[:connect_timeout]}ms")
          end
        end
      rescue SystemCallError => ex
        close
        raise ConnectionError.new("#{ex.class}: #{ex.message}")
      end

      true
    end

    # Reads the next message from the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Neptune::Buffer]
    def read(correlation_id)
      # Determine the time by which a response must be read
      deadline = Time.now + (@config[:read_timeout] / 1000.0)

      buffer = nil
      until buffer
        seconds_left = (deadline - Time.now).to_f

        if seconds_left > 0 || ready_for_read?
          # Attempt to read the next buffer
          next_buffer, next_correlation_id = read_buffer(seconds_left)
          if correlation_id == next_correlation_id
            buffer = next_buffer
          end
        else
          # Time has run out
          raise Errno::ETIMEDOUT.new("exceeded read timeout of #{@config[:read_timeout]}ms")
        end
      end

      buffer
    rescue SystemCallError, IOError => ex
      close
      raise ConnectionError.new("#{ex.class}: #{ex.message}")
    end

    # Writes the given data to the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Boolean] true, always
    def write(data)
      begin
        if ready_for_write?(@config[:write_timeout])
          # There is a potential race condition here, where the socket could time out between IO Select and Write
          # This will throw a Errno::ETIMEDOUT, but it will be caught in the calling code
          @socket.write(data)
        else
          raise Errno::ETIMEDOUT.new("exceeded write timeout of #{@config[:write_timeout]}ms")
        end
      rescue SystemCallError => ex
        close
        raise ConnectionError.new("#{ex.class}: #{ex.message}")
      end

      true
    end

    # Closes the socket.  It can no longer be read / written to after this until
    # it reconnects.
    # @return [Boolean] true, always
    def close
      if @socket
        begin
          @socket.close
        rescue IOError => ex
          # Ignore any error
        ensure
          @socket = nil
        end
      end

      true
    end

    private
    # Reads the next message buffer from the socket
    # @return [Neptune::Buffer, Fixnum]
    def read_buffer(timeout)
      length = read_bytes(4, timeout).unpack('N').first
      buffer = Buffer.new(read_bytes(length, timeout))
      correlation_id = Types::Int32.from_kafka(buffer)
      [buffer.rewind, correlation_id]
    end

    # Reads the specified number of bytes from the socket
    # @return [String]
    def read_bytes(maxlen, timeout)
      if ready_for_read?(timeout)
        data = @socket.read(maxlen)
        raise EOFError.new('end of file reached') unless data
        data
      else
        raise Errno::ETIMEDOUT.new("exceeded read timeout of #{@config[:read_timeout]}ms")
      end
    end
  end
end