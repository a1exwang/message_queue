require_relative 'log'
require 'timeout'

class MessageQueue
  module Error
    UnknownMessageError = Class.new(Exception)
    UnHandledMessageError = Class.new(Exception)
    DuplicateHandlerError = Class.new(Exception)
  end

  # :initialized, :running, :stopped
  attr_reader :status

  DEFAULT_CPU_WORKER_COUNT = 4
  DEFAULT_IO_WORKER_COUNT = 32
  def init_workers(cpu_worker_count, io_worker_count)
    raise ArgumentError unless cpu_worker_count.is_a?(Integer) && io_worker_count.is_a?(Integer)
    @cpu_worker_queue = Queue.new
    @cpu_workers = Array.new(cpu_worker_count) do |i|
      Thread.new do
        loop do
          item = @cpu_worker_queue.deq
          job = item[:job]
          callback = item[:callback]
          raise 'Invalid job in cpu queue' unless job
          result = job.call
          callback.call(result) if callback
        end
      end
    end

    @io_worker_queue = Queue.new
    @io_workers = Array.new(io_worker_count) do |i|
      Thread.new do
        loop do
          item = @io_worker_queue.deq
          job = item[:job]
          callback = item[:callback]
          raise 'Invalid job in io queue' unless job
          result = job.call
          callback.call(result) if callback
        end
      end
    end

  end
  ##
  # MessageQueue#add_cpu_job(job, &callback)
  #   Add a job to worker queue.
  #   The +job+ is executed concurrently, so you can only call MessageQueue#<< or send_delay method.
  # @param job: Proc object (or any object that responds to 'call'), executed in worker thread concurrently.
  # @param callback: Proc object(or any object that responds to 'call').
  #   Called after job is done.
  #   Gives on parameter, callback.call(+result+), +result+ is the job return value.
  #   Executed in message thread.
  #
  # This method is thread safe.
  def add_cpu_job(job, &callback)
    @cpu_worker_queue << { job: job, callback: callback }
  end

  def add_io_job(job, &callback)
    @io_worker_queue << { job: job, callback: callback }
  end

  ##
  # create a message queue and set handlers with block
  # @param block
  #
  # examples:
  #   MessageQueue.new do |handler|
  #     handler.on_req(lambda) do |m|
  #       # do something
  #     end
  #     handler.on(name) do |m|
  #       # do something
  #     end
  #     handler.on(name, lambda) do |m|
  #     end
  # message_handler = { name: 'a', filter: , handler:, ... }
  def initialize(cpu_worker_count = DEFAULT_CPU_WORKER_COUNT, io_worker_count = DEFAULT_IO_WORKER_COUNT, &block)
    @msg_queue = Queue.new
    @handlers = {}
    @status = :initialized
    add_handlers(&block) if block
    init_workers(cpu_worker_count, io_worker_count)
  end

  ##
  # similar to +MessageQueue.new+
  def add_handlers(&block)
    if @status == :running
      raise ArgumentError, 'cannot add handler to a running queue'
    end
    obj = MyObject.new
    block.call(obj)
    @handlers.merge!(obj.message_handlers) do |key, _val1, _val2|
      raise DuplicateHandlerError, "handler '#{key}' already defined"
    end
  end

  ##
  # add a message and execute without delay
  # @param: message, a hash object
  # note:
  #   these message names has special meanings and do not use them.
  #   '__quit'
  #   '__block'
  def <<(message)
    raise ArgumentError unless message.is_a?(Hash)
    @msg_queue << message
  end

  def send_delay(seconds, message)
    raise ArgumentError unless seconds.is_a? Numeric
    Timeout.timeout(seconds) do
      self << message
    end
  end

  def send_block(&block)
    raise ArgumentError unless block
    self << {
        name: '__block',
        __block: block
    }
  end

  def clear
    @msg_queue.clear
    @cpu_worker_queue.clear
    @io_worker_queue.clear
  end

  def clear_worker
    @cpu_worker_queue.clear
    @io_worker_queue.clear
  end

  def quit
    # if @status == :running
      self << { name: '__quit' }
    # end
  end

  def start_blocking
    @status = :running
    loop do
      # begin
      message = @msg_queue.pop
      # rescue Exception => e
      #   LOG.logt('MessageQueue', "@queue.pop exception '#{e.inspect}'")
      #   LOG.logt('MessageQueue', 'Queue empty with no workers working, job done!')
      #   break
      # end

      name = message[:name].to_s
      case name
        when '__block'
          message[:__block].call(message)
        when '__quit'
          @cpu_workers.size.times do
            add_cpu_job(lambda { Thread.kill(Thread.current) })
          end
          @io_workers.size.times do
            add_io_job(lambda { Thread.kill(Thread.current) })
          end
          LOG.logt('MessageQueue', 'Quiting...')
          break
        else
          if @handlers[name]
            handled = false
            @handlers[name].each do |handler|
              if !handler[:filter] || handler[:filter].call(message)
                handler[:handler].call(message)
                handled = true
                break
              end
            end
            LOG.logt 'Message Queue', "message unhandled #{message}" unless handled
          else
            LOG.logt 'Message Queue', "unknown message #{message}"
          end
      end
    end
    @status = :stopped
  end

  private
  class MyObject
    attr_reader :message_handlers
    def initialize
      @message_handlers = Hash.new { Array.new }
    end
    def on(name, lambda = nil, &block)
      @message_handlers[name] += [{ filter: lambda, handler: block }]
    end
    # define on_xxx methods
    def method_missing(name, *params, &block)
      if name.to_s =~ /on_(.+)$/
        message_name = $1
        on(message_name, *params, &block)
      else
        super
      end
    end
  end
end

queue = MessageQueue.new do |event|
  event.on('message') do
  end
end
queue << { name: 'message' }
queue.quit
queue.start_blocking

queue = MessageQueue.new do |event|
  event.on('message') do
    LOG.log('message')
  end
end
queue << { name: 'message' }
queue.quit
queue.start_blocking