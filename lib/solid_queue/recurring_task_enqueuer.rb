# frozen_string_literal: true

module SolidQueue
  class RecurringTaskEnqueuer < Processes::Base
    include Processes::Runnable

    attr_reader :recurring_tasks, :task_pool

    def initialize(recurring_tasks)
      @recurring_tasks = recurring_tasks
    end

    private
      def schedule_task(recurring_task, previous_time: nil)
        due_at = recurring_task.next_due(previous_time:)
        delay = [(due_at - Time.current), 0].max

        future = Concurrent::ScheduledTask.new(delay, args: [recurring_task, due_at], executor:) do |recurring_task, due_at|
          schedule_task(recurring_task, previous_time: due_at)

          recurring_task.enqueue
        end

        future.execute
      end

      def boot
        super

        recurring_tasks.each { |recurring_task| schedule_task recurring_task }
      end

      def run
        # We have no need for polling, so just perform a sleep
        interruptible_sleep(1.hour)
      end

      def shutdown
        super

        executor.kill
      end

      def executor
        @executor ||= Concurrent::SingleThreadExecutor.new
      end
  end
end
