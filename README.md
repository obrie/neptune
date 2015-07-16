# Neptune

Neptune is a Kafka client. It's a distant fork of the [Poseidon](http://github.com/bpot/poseidon)
client.

Features currently implemented:
* Metadata
* Produce

Upcoming features:
* Compression
* Fetch
* Offset management

## Background

Why a new Kafka library for Ruby?  These are some areas I was looking to improve upon:
* Active maintenance
* Code design / clarity
* Error handling
* API cleanup

## Usage

```ruby
require 'neptune'

cluster = Neptune::Cluster.new(['localhost:9092'], client_id: 'my_test_producer')
cluster.topic('topic1')
cluster.produce('topic1', 'value1')
cluster.produce!('topic1', 'value1', 'key1')
cluster.batch do
  cluster.produce('topic1', 'value1')
  cluster.produce('topic1', 'value1', 'key1')
end
```

## Requirements

* Ruby 1.9.3 or higher
* Kafka 0.8 or higher