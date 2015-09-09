module Neptune
  module Support
    # Provides access to a Hash-like object that can allow callers to block until
    # the key appears
    class BlockingHash
      def initialize
        @hash = {}
        @waiters = {}
        @lock = Mutex.new
      end

      # Looks up the given key in this hash. If a timeout is provided, then
      # this will block up until the given time waiting for the key to appear.
      # @return [Object]
      def [](key, timeout = nil)
        # Attempt to get the key without locking to optimize access time
        get(key) || @lock.synchronize do
          get(key) || begin
            raise(@exception) if @exception

            # Wait for the key to become available
            @waiters[key] = Thread.current
            @lock.sleep(timeout)
            get(key)
          ensure
            @waiters.delete(key)
          end
        end
      end

      # Associates the key with the given value.  Any thread waiting on the key
      # will be notified that it's now present.
      def []=(key, value)
        @hash[key] = value
        @lock.synchronize do
          # Wake up any thread waiting for the key
          if waiter = @waiters[key]
            begin
              waiter.wakeup
            rescue ThreadError
              # Ignore
            end
            @waiters.delete(key)
          end
        end
      end

      # Retrieves the given key.  Once the key is found, it cannot be looked up
      # again
      # @return [Object]
      def get(key)
        @hash.delete(key)
      end

      # Raises a permanent exception on all threads currently waiting for keys
      # *and* all future threads that attempt to wait for keys.
      def raise(ex)
        return if @exception

        @exception = ex
        @lock.synchronize do
          @waiters.each do |key, waiter|
            waiter.raise(ex)
          end
        end
      end
    end
  end
end