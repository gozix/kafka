# GoZix Kafka

## Dependencies

* [zap](https://github.com/gozix/zap)
* [prometheus](https://github.com/gozix/prometheus)

## Built-in DI options

| Name             | Description          | 
|------------------|----------------------|
| AsKafkaListener  | Add a kafka listener |

## Kafka Publisher

To create a new Kafka Publisher, use `usage.NewPublisherWithName(name, config)`.
If `config` is `nil`, the default configuration will be used.

### Example:

```go
di.Provide(usage.NewPublisherWithName(client.DEFAULT, nil), di.Tags{{Name: client.DEFAULT}})
