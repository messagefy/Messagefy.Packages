using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Messagefy.Abstractions.MessageBus;
using Messagefy.Kafka.Configuration;

namespace Messagefy.Kafka;

/// <summary>
/// Kafka producer implementation for sending messages to a Kafka topic.
/// </summary>
/// <typeparam name="TMessage">The type of message that this producer will handle.</typeparam>
public sealed class KafkaProducer<TMessage> : IMessageBusProducer<TMessage>
    where TMessage : class
{
    private readonly KafkaProducerOptions _producerOptions;
    private readonly IProducer<Null, TMessage> _producer;
    private readonly ISchemaRegistryClient _schemaRegistryClient;

    public KafkaProducer(KafkaProducerOptions producerOptions)
    {
        _producerOptions = producerOptions;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = producerOptions.BootstrapServers,
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = producerOptions.SchemaRegistryUrl
        };

        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
        _producer = new ProducerBuilder<Null, TMessage>(producerConfig)
            .SetValueSerializer(new JsonSerializer<TMessage>(_schemaRegistryClient, new JsonSerializerConfig()))
            .Build();
    }
    
    /// <summary>
    /// Releases the resources used by the KafkaProducer instance asynchronously.
    /// </summary>
    /// <returns>A ValueTask that completes when the resources have been released.</returns>
    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        _schemaRegistryClient.Dispose();
        
        return default;
    }
    
    /// <summary>
    /// Asynchronously produces a message to the Kafka topic specified in the producer options.
    /// </summary>
    /// <param name="message">The message to be produced.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous produce operation.</returns>
    /// <exception cref="ProduceException{Null,TMessage}">Thrown if an error occurs during the produce operation.</exception>
    public async Task ProduceAsync(TMessage message, CancellationToken cancellationToken = default)
    {
        var kfMessage = new Message<Null, TMessage> { Value = message };

        await _producer.ProduceAsync(_producerOptions.Topic, kfMessage, cancellationToken);
    }
}