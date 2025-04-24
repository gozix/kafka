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

if you have one default publisher
```go
di.Provide(kafkaapi.NewPublisherWithName(client.DEFAULT, nil)),
```

if you have more than one
```go
type (
    DefaultPublisher kafkaapi.Publisher
    Custom0Publisher kafkaapi.Publisher
    Custom1Publisher kafkaapi.Publisher
    ...
    CustomNPublisher kafkaapi.Publisher
)
...
di.Provide(kafkaapi.NewPublisherWithName(client.DEFAULT, nil), di.As(new(DefaultPublisher))),
di.Provide(kafkaapi.NewPublisherWithName("custom0", nil), di.As(new(Custom0Publisher))),
di.Provide(kafkaapi.NewPublisherWithName("custom1", nil), di.As(new(Custom1Publisher))),
...
di.Provide(kafkaapi.NewPublisherWithName("customN", nil), di.As(new(CustomNPublisher))),

