require 'socket'
require 'openssl'

module Neptune
  module Support
    # A wrapper around Socket to provide higher-level functionality such as
    # timeouts
    class Socket
      # Hostname of the remote server
      # @return [String]
      attr_reader :host

      # Port number of the remote server
      # @return [Fixnum]
      attr_reader :port

      # Configuration for the socket
      # @return [Hash]
      attr_accessor :config

      def initialize(host, port, config = {}) #:nodoc:
        @host = host
        @port = port
        @config = {
          connect_timeout: 60_000,
          read_timeout: 60_000,
          write_timeout: 60_000
        }.merge(config)
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

      # Waits the given amount of seconds until the socket is ready to be read from
      # @return [Boolean]
      def ready_for_read?(timeout = 0)
        @socket && IO.select([@socket], nil, nil, timeout ? timeout / 1000.0 : nil)
      end

      # Waits the given amount of seconds until the socket is ready to be written to
      # @return [Boolean]
      def ready_for_write?(timeout = 0)
        @socket && IO.select(nil, [@socket], nil, timeout ? timeout / 1000.0 : nil)
      end

      # Re-opens a connection to the remote server, closing any that may be
      # currently open
      # @raise [SystemCallError, OpenSSL::SSL::SSLError] if the socket cannot connect
      # @return [Boolean] true, always
      def reconnect
        close unless closed?
        connect
      end

      # Opens a connection to the remote server
      # @raise [SystemCallError, OpenSSL::SSL::SSLError] if the socket cannot connect
      # @return [Boolean] true, always
      def connect
        address = ::Socket.getaddrinfo(host, nil)
        socket_address = ::Socket.pack_sockaddr_in(port, address[0][3])

        @socket = ::Socket.new(::Socket.const_get(address[0][0]), ::Socket::SOCK_STREAM, 0)
        begin
          @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)

          try_or_wait(read: config[:connect_timeout], write: config[:connect_timeout]) do
            begin
              @socket.connect_nonblock(socket_address)
            rescue Errno::EISCONN
              # Socket is connected
            end
          end
        rescue SystemCallError => ex
          close
          raise
        end

        start_ssl if config[:ssl]

        true
      end

      # Reads the given number of bytes from the socket.
      # @raise [SystemCallError] if the socket is unavailable or has timed out
      # @return [String]
      def read(length, options = {})
        options = {timeout: config[:read_timeout]}.merge(options)

        data = ""
        while data.length < length
          try_or_wait(read: options[:timeout]) do
            next_data = @socket.read_nonblock(length - data.length)
            raise EOFError.new('end of file reached') unless next_data
            data << next_data
          end
        end
        data
      end

      # Writes the given data to the socket.
      # @raise [SystemCallError] if the socket is unavailable or has timed out
      # @return [Boolean] true, always
      def write(data, options = {})
        options = {timeout: config[:write_timeout]}.merge(options)

        try_or_wait(write: options[:timeout]) do
          @socket.write_nonblock(data)
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
      # Starts an SSL connection through the current socket
      # @raise [SystemCallError, OpenSSL::SSL::SSLError]
      def start_ssl
        ssl_socket = OpenSSL::SSL::SSLSocket.new(@socket, ssl_context)
        ssl_socket.sync_close = true
        try_or_wait(read: config[:connect_timeout], write: config[:connect_timeout]) do
          ssl_socket.connect_nonblock
        end

        @socket = ssl_socket
      end

      # Generates the configuration context for an SSL socket
      # @raise [OpenSSL::SSL::SSLError]
      # @return [OpenSSL::SSL::SSLContext]
      def ssl_context
        context = OpenSSL::SSL::SSLContext.new
        context.cert = OpenSSL::X509::Certificate.new(File.open(config.fetch(:ssl_cert_file)))
        context.key = OpenSSL::PKey::RSA.new(File.open(config.fetch(:ssl_key_file)))
        context.ca_file = config[:ssl_ca_file] if config[:ssl_ca_file]
        if config[:ssl_verify]
          context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          context.verify_callback = lambda do |preverify_ok, context|
            if preverify_ok != true || context.error != 0
              raise OpenSSL::SSL::SSLError.new("SSL Verification failed -- Preverify: #{preverify_ok}, Error: #{context.error_string} (#{context.error})")
            end
          end
        else
          context.verify_mode = OpenSSL::SSL::VERIFY_NONE
        end
        context
      end

      # Runs the given block, waiting for the read / write streams to become
      # available if an exception is raised indicating that they aren't.
      # 
      # @raise [Errno::ETIMEDOUT] if the given timeout has exceeded
      # @return the return value from the block
      def try_or_wait(timeouts = {})
        timeouts = {read: 0, write: 0}.merge(timeouts)

        begin
          yield
        rescue IO::WaitReadable
          if ready_for_read?(timeouts[:read])
            retry
          else
            raise Errno::ETIMEDOUT.new('exceeded read timeout')
          end
        rescue IO::WaitWritable
          if ready_for_write?(timeouts[:write])
            retry
          else
            raise Errno::ETIMEDOUT.new('exceeded write timeout')
          end
        end
      end
    end
  end
end