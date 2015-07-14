require 'socket'
require 'neptune/buffer'
require 'neptune/errors'

module Neptune
  # A socket connection to a node in the cluster
  class Connection
    # Hostname of the node
    # @return [String]
    attr_reader :host

    # Port number of the node
    # @return [Fixnum]
    attr_reader :port

    def initialize(host, port, config) #:nodoc:
      @host = host
      @port = port
      @config = config
    end

    # Determines whether the socket is open and can be read from
    # @return [Boolean]
    def alive?
      !closed? && (!IO.select([@socket], nil, nil, 0) || !@socket.eof?)
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
          if IO.select(nil, [@socket], nil, @config[:connect_timeout] / 1000.0)
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
        raise ConnectionError.new("#{ex.class}: #{ex.message}", ex)
      end

      true
    end

    # Reads the next messages from the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Neptune::Buffer]
    def read
      length = read_bytes(4).unpack('N').first
      Buffer.new(read_bytes(length))
    rescue SystemCallError, IOError => ex
      close
      raise ConnectionError.new("#{ex.class}: #{ex.message}", ex)
    end

    # Writes the given data to the socket.
    # @raise [ConnectionError] if the socket is unavailable or has timed out
    # @return [Boolean] true, always
    def write(data)
      begin
        if IO.select(nil, [@socket], nil, @config[:write_timeout] / 1000.0)
          # There is a potential race condition here, where the socket could time out between IO Select and Write
          # This will throw a Errno::ETIMEDOUT, but it will be caught in the calling code
          @socket.write(data)
        else
          raise Errno::ETIMEDOUT.new("exceeded write timeout of #{@config[:write_timeout]}ms")
        end
      rescue SystemCallError => ex
        close
        raise ConnectionError.new("#{ex.class}: #{ex.message}", ex)
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
    # Reads the specified number of bytes from the socket
    def read_bytes(maxlen)
      if IO.select([@socket], nil, nil, @config[:read_timeout] / 1000.0)
        data = @socket.read(maxlen)
        raise EOFError.new('end of file reached') unless data
        data
      else
        raise Errno::ETIMEDOUT.new("exceeded read timeout of #{@config[:read_timeout]}ms")
      end
    end
  end
end