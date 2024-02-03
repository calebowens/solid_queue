# frozen_string_literal: true

module SolidQueue
  class CronEnqueuer < Processes::Base
    include Processes::Runnable

    attr_reader :cron_tasks, :task_pool

    def initialize(cron_tasks)
      @cron_tasks = cron_tasks

      @task_pool = Concurrent::Hash.new
    end

    private
      def schedule_task(cron_task, previous_time: nil)
        due_at = cron_task.next_due(preivous_time)
        delay = [(due_at - Time.current), 0].max

        future = Concurrent::ScheduledTask.new(delay, [cron_task, due_at]) do |cron_task, due_at|
          schedule_task(cron_task, previous_time: due_at)

          cron_task.enqueue
        end

        task_pool[cron_task.key] = future
        future.execute
      end

      def run
        cron_tasks.each { |cron_task| schedule_task cron_task }
      end

      def shutdown
        super

        task_pool.each { |_, task| task.cancel }
        task_pool.clear
      end
  end
end
