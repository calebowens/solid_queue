# frozen_string_literal: true

module SolidQueue
  class Semaphore < Record
    scope :available, -> { where("value > 0") }
    scope :expired, -> { where(expires_at: ...Time.current) }

    class << self
      def wait(job)
        Proxy.new(job).wait
      end

      def signal(job)
        Proxy.new(job).signal
      end

      def signal_all(jobs)
        Proxy.signal_all(jobs)
      end
    end

    class Proxy
      def self.signal_all(jobs)
        Semaphore.where(key: jobs.map(&:concurrency_key)).update_all("value = value + 1")
      end

      def initialize(job)
        @job = job
        @retries = 0
      end

      def wait
        if semaphore = Semaphore.find_by(key: key)
          semaphore.value > 0 && attempt_decrement
        else
          attempt_creation
        end
      end

      def signal
        attempt_increment
      end

      private
        attr_accessor :job, :retries

        def attempt_creation
          Semaphore.create!(key: key, value: limit - 1, expires_at: expires_at)
          true
        rescue ActiveRecord::RecordNotUnique
          attempt_decrement
        end

        def attempt_decrement
          Semaphore.available.where(key: key).update_all([ "value = value - 1, expires_at = ?", expires_at ]) > 0
        rescue ActiveRecord::Deadlocked
          if retriable? then attempt_retry
          else
            raise
          end
        end

        def attempt_increment
          Semaphore.where(key: key, value: ...limit).update_all([ "value = value + 1, expires_at = ?", expires_at ]) > 0
        end

        def attempt_retry
          self.retries += 1

          if semaphore = Semaphore.find_by(key: key)
            semaphore.value > 0 && attempt_decrement
          end
        end

        MAX_RETRIES = 1

        def retriable?
          retries < MAX_RETRIES
        end

        def key
          job.concurrency_key
        end

        def expires_at
          job.concurrency_duration.from_now
        end

        def limit
          job.concurrency_limit
        end
    end
  end
end
