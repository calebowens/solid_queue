# frozen_string_literal: true

module SolidQueue
  class CronEnqueuer < Processes::Base
    include Processes::Runnable

    attr_reader :cron_tasks, :task_pool

    def initialize(cron_tasks)
      @cron_tasks = cron_tasks
    end

    private
      def schedule_task(cron_task, previous_time: nil)
        due_at = cron_task.next_due(previous_time:)
        delay = [(due_at - Time.current), 0].max

        future = Concurrent::ScheduledTask.new(delay, args: [cron_task, due_at], executor:) do |cron_task, due_at|
          schedule_task(cron_task, previous_time: due_at)

          cron_task.enqueue
        end

        future.execute
      end

      def boot
        super

        cron_tasks.each { |cron_task| schedule_task cron_task }
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
