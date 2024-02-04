class PingJob < ApplicationJob
  def perform
    Rails.logger.info "pong!"
  end
end
