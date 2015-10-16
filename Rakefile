require "bundler/gem_tasks"

require 'sqs'
require 'sqs/worker'

Aws.config[:region] = 'eu-west-2'
Aws.config[:credentials] = Aws::Credentials.new(
  'KEY',
  'SECRET'
)

task :default do
  worker = Sqs::Worker.new(
    queue: "development_default",
    worker_size: 2
  )

  worker.execute do |message|
    p message.body

    sleep 1
  end
end
