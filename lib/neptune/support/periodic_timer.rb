require 'thread'

module Neptune
  module Support
    class PeriodicTimer
      # The time at which the callback was last run
      # @return [Time]
      attr_reader :last_run

      # The amount of time to wait (in ms) between invocations of the callback
      # @return [Fixnum]
      attr_reader :interval

      def initialize(interval, &callback) #:nodoc:
        @interval = interval
        @callback = callback
        @stopped = false

        @notifier = ConditionVariable.new
        @lock = Mutex.new

        # Start a background thread
        @runner = Thread.new { run }
      end

      # The number of seconds until the next automatic refresh of cluster metadata
      # will occur
      # @return [Fixnum]
      def seconds_left
        if @last_run
          [(@last_run + interval / 1000.0) - Time.now, 0].max
        else
          interval / 1000.0
        end
      end

      # Resets the timer
      # @return [Boolean] true, always
      def reset
        @last_run = Time.now
        true
      end

      # Stops the timer, waiting for any backgound processes to complete
      # processing
      # @return [Boolean] true, always
      def stop
        @stopped = true

        # Wait for the background thread to complete
        @lock.synchronize { @notifier.signal }
        @runner.join

        true
      end

      # Whether the timer has been stoped
      # @return [Boolean]
      def stopped?
        @stopped
      end

      private
      # Runs a continuous loop, invoking the configured callback every interval
      # milliseconds
      def run
        @lock.synchronize do
          until stopped?
            if run?
              begin
                @callback.call
              rescue => ex
                logger.warn "[Neptune] Failed to run timer: #{ex.message}"
              ensure
                # Always reset, regardless of whether it failed
                reset
              end
            else
              # Wait until enough time has passed for the next invocation
               @notifier.wait(@lock, seconds_left)
            end
          end
        end
      end

      # Whether the timer is ready to run the callback
      # @return [Boolean]
      def run?
        seconds_left == 0
      end
    end
  end
end