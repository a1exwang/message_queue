require 'message_queue'

RSpec.describe MessageQueue do

  context 'method #<<' do

    it 'receives a named message' do
      message_received = false
      queue = MessageQueue.new do |event|
        event.on('message') do
          message_received = true
        end
      end
      queue << { name: 'message' }
      queue.quit
      expect(message_received).to eq false
      queue.start_blocking
      expect(message_received).to eq true
    end

    it 'executes in cpu worker queue' do
      execute_in_cpu_worker_queue = false
      callback_executed = false
      queue = MessageQueue.new {}
      queue.add_cpu_job(lambda { execute_in_cpu_worker_queue = true }) do
        callback_executed = true
      end
      queue.quit
      queue.start_blocking
      expect(callback_executed).to eq true
      expect(execute_in_cpu_worker_queue).to eq true
    end

    it 'executes in io worker queue' do
      callback_ok = false
      execute_ok = false
      queue = MessageQueue.new {}
      queue.add_io_job(lambda { execute_ok = true }) do
        callback_ok = true
      end
      queue.quit
      queue.start_blocking
      expect(execute_ok).to eq true
      expect(callback_ok).to eq true
    end

  end
end

