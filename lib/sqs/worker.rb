require 'logger'
require 'thread/pool'
require 'thread/channel'

require 'aws-sdk'

Thread.abort_on_exception = true

module Sqs
  class Worker
    attr_reader :logger

    def initialize(options)
      @options = {
        limit: 10,
        wait: false,
        poller_size: 1,
        idle_timeout: 20,
        wait_time_seconds: 20,
        logger: Logger.new(STDOUT)
      }.merge(options)

      @pool   = Thread.pool 1, @options[:worker_size]
      @poller = build_poller
      @logger = @options[:logger]
    end

    def execute(&block)
      subscribe_to_messages(&block)
      trap_signals

      loop do
        sleep 5
      end
    end

    private

    def build_poller
      response = Aws::SQS::Client.new.get_queue_url(queue_name: @options[:queue])

      poller_options = {
        skip_delete: true,
        max_number_of_messages: @options[:limit],
        wait_time_seconds:      @options[:wait_time_seconds],
        idle_timeout:           @options[:idle_timeout]
      }

      Aws::SQS::QueuePoller.new(response.queue_url, poller_options)
    end

    def subscribe_to_messages(&block)
      @poller_threads = @options[:poller_size].times.map do
        Thread.new do
          loop do
            @poller.poll do |messages|
              handle messages, &block
            end

            sleep 0.1
          end
        end
      end
    end

    def handle(messages, &block)
      messages.each do |message|
        break if @shutdown

        @pool.process do
          begin
            unless @shutdown
              block[message]

              @poller.delete_message(message)
            end
          rescue StandardError => e
           logger.error "[SQS] Error occured: #{e.message}"
           receive_count = message.attributes['ApproximateReceiveCount'].to_i

           timeout = case receive_count
           when 1
             30
           when 2..3
             receive_count * 60
           else
             receive_count * 180
           end

           logger.error "[SQS] Change visibility: #{timeout}"
           begin
             @poller.change_message_visibility_timeout(message, timeout)
           rescue StandardError => e
             logger.error "[SQS] Change visibility failed: #{e.message}"
           end

           raise e
         end
        end
      end

      @pool.wait if @options[:wait]
    end

    def trap_signals
      trap 'TTIN' do
        Thread.new do
          logger.debug "   Thread count: #{Thread.list.count}"

          logger.debug "   Thread inspections:"
          Thread.list.each do |thread|
            logger.debug "    #{thread.object_id}: #{thread.status}"
          end

          logger.debug "   GC stats:"
          logger.debug GC.stat

          logger.debug "   Object Space:"

          counts = Hash.new{ 0 }
          ObjectSpace.each_object do |o|
            counts[o.class] += 1
          end

          logger.debug counts
        end
      end

      %w(SIGTERM INT).each do |signal|
        trap signal do
          logger.info "[SQS] Receive #{signal} signal. Shutting down pool..."

          Thread.new do
            @shutdown = true

            @pool.shutdown
            (@poller_threads || []).map(&:exit)

            logger.info "[SQS] All done. Exit..."
            Process.exit(0)
          end
        end
      end
    end
  end
end
