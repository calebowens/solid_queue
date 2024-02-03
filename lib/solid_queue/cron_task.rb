# frozen_string_literal: true

module SolidQueue
  class CronTask
    include ActiveModel::Model, AppExecutor

    attr_accessor :args, :kwargs, :job, :set, :schedule

    validates :schedule, presence: true
    validates :job, presence: true
    validate :ensure_schedule_valid
    validate :ensure_job_valid
    validate :ensure_args_valid
    validate :ensure_kwargs_valid
    validate :ensure_set_valid

    def enqueue
      wrap_in_app_executor do
        job_class
          .set(**set)
          .perform_later(*args, **(kwargs || {}))
      end
    end

    def next_due(previous_time: nil)
      cron_schedule.next_time(previous_time).to_t
    end

    def key
      job + schedule
    end

    private
      def cron_schedule
        @cron_schedule ||= Fugit.parse_cron(schedule)
      end

      def job_class
        @job_class ||= wrap_in_app_executor do
          job.constantize
        end
      rescue NameError
        nil
      end

      def ensure_schedule_valid
        if cron_schedule.nil?
          errors.add :schedule, :not_valid, message: "invalid format"
        end
      end

      def ensure_job_valid
        if job_class.nil?
          errors.add :job_class, :not_valid, message: "class name invalid or undefined"
        end
      end

      def ensure_args_valid
        return if args.nil?

        unless args.is_a? Array
          erorrs.add :args, :not_array, message: "should be an array"
        end
      end

      def ensure_kwargs_valid
        return if kwargs.nil?

        unless kwargs.is_a? Hash
          erorrs.add :kwargs, :not_hash, message: "should be a hash"
        end
      end

      def ensure_set_valid
        return if set.nil?

        unless set.is_a? Hash
          erorrs.add :set, :not_hash, message: "should be a hash"
        end
      end
  end
end
