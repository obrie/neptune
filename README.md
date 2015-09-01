# Neptune

Neptune is a Kafka client. It's a distant fork of the [Poseidon](http://github.com/bpot/poseidon)
client.

Features currently implemented:
* Metadata
* Produce
* Compression
* Fetch
* Offset management
* SSL

## Background

Why a new Kafka library for Ruby?  These are some areas I was looking to improve upon:
* Active maintenance
* Code design / clarity
* Error handling
* API cleanup

While this originated as a fork of [Poseidon](http://github.com/bpot/poseidon), the
implementation has changed vastly.

## Usage

```ruby
require 'neptune'

cluster = Neptune::Cluster.new(['localhost:9092'], client_id: 'my_test_producer')

cluster.topic('topic1')

cluster.known_topics
cluster.known_topics!

cluster.produce('topic1', 'value1')
cluster.produce('topic1', 'value1', required_acks: 1, ack_timeout: 1000)
cluster.produce!('topic1', 'value1', key: 'key1')

cluster.batch(:produce) do |batch|
  batch.produce('topic1', 'value1')
  batch.produce('topic1', 'value1', key: 'key1')
end

cluster.batch(:produce, required_acks: 1) do |batch|
  batch.produce('topic1', 'value1')
  batch.produce('topic1', 'value1', key: 'key1')
end

cluster.fetch('topic1', 0, 0)
cluster.fetch('topic1', 0, 0, max_wait_time: 1000, min_bytes: 1024)
cluster.fetch!('topic1', 0, 0)
cluster.fetch!('topic1', 0, 0, max_wait_time: 1000, min_bytes: 1024)

cluster.batch(:fetch) do |batch|
  batch.fetch('topic1', 0, 0)
  batch.fetch('topic2', 0, 0)
end

cluster.offset('topic1', 0, time: :earliest)
cluster.offset('topic1', 0, time: :latest)
cluster.offset('topic1', 0, time: Time.now.to_i - 1000)

cluster.batch(:offset) do |batch|
  batch.offset('topic1', 0, time: :earliest)
  batch.offset('topic1', 0, time: :latest)
end

cluster.batch(:offset) do |batch|
  batch.offset('topic1', 0, time: :earliest)
  batch.offset('topic1', 0, time: :latest)
end

cluster.coordinator
cluster.coordinator(group: 'name')
cluster.coordinator!
cluster.coordinator!(group: 'name')

cluster.offset_fetch('topic1', 0)
cluster.offset_fetch('topic1', 0, group: 'name')
cluster.offset_fetch!('topic1', 0, group: 'name')

cluster.batch(:offset_fetch, group: 'name') do |batch|
  batch.offset_fetch('topic1', 0)
  batch.offset_fetch('topic1', 1)
end

cluster.offset_commit('topic1', 0, 1000)
cluster.offset_commit('topic1', 0, 1000, group: 'name')
cluster.offset_commit!('topic1', 0, 1000)

cluster.batch(:offset_commit, group: 'name') do |batch|
  batch.offset_commit('topic1', 0, 1000)
end
```

## To do

* Stream API
* CLI
* Heartbeat / JoinGroup APIs

## Requirements

* Ruby 1.9.3 or higher
* Kafka 0.8 or higher
