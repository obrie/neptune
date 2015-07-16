# Neptune

Neptune is a Kafka client. It's a fork of the [Poseidon](http://github.com/bpot/poseidon) client.

The only features that are currently implemented include:
* Metadata
* Produce

To be implemented:
* Compression
* Fetch
* Offset management

## Usage

```ruby
require 'neptune'

cluster = Neptune::Cluster.new(['localhost:9092'], client_id: 'my_test_producer')
cluster.topic('topic1', 'value1')
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