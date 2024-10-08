using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Messagefy.Abstractions.MessageBus;
using Messagefy.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Messagefy.Kafka;

/// <summary>
/// KafkaConsumer class for consuming messages from Kafka topics using Confluent.Kafka library.
/// </summary>
/// <typeparam name="TMessage">Type of the message that will be consumed.</typeparam>
public sealed class KafkaConsumer<TMessage> : IMessageBusConsumer<TMessage> where TMessage : class
{
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly IConsumer<Ignore, TMessage> _consumer;

    private readonly KafkaConsumerOptions _options;
    private readonly ILogger<KafkaConsumer<TMessage>> _logger;

    public KafkaConsumer(KafkaConsumerOptions options, ILogger<KafkaConsumer<TMessage>> logger)
    {
        _options = options;
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId
        };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = options.SchemaRegistryUrl
        };
        
        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _consumer = new ConsumerBuilder<Ignore, TMessage>(config)
            .SetValueDeserializer(new JsonDeserializer<TMessage>()
                .AsSyncOverAsync())
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Consumer error: {e.Reason}");
            })
            .SetStatisticsHandler((_, json) => _logger.LogInformation("Statistics: {KafkaStatistics}", json))
            .Build();
    }
    
    /// <summary>
    /// Disposes the Kafka consumer and schema registry client asynchronously.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        _consumer.Dispose();
        _schemaRegistryClient.Dispose();
        
        return default;
    }
    
    /// <summary>
    /// Consumes messages from the specified Kafka topic asynchronously.
    /// </summary>
    /// <param name="handler">The handler function to process each consumed message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous consume operation.</returns>
    /// <remarks>
    /// This method subscribes to the specified Kafka topic and processes incoming messages using the provided handler function.
    /// If <c>EnableAutoCommit</c> is set to <c>false</c>, it will manually commit offsets based on the <c>CommitOffsetPeriod</c>.
    /// Handles <see cref="ConsumeException"/> and generic exceptions, logging them appropriately.
    /// </remarks>
    /// <exception cref="Exception">Throws an exception if an unhandled error occurs during the consume process.</exception>
    public async Task ConsumeAsync(
        Func<TMessage, CancellationToken, Task> handler, CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        _logger.LogInformation($"Consuming messages... from topic: {_options.Topic}");
        _consumer.Subscribe(_options.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        continue;
                    }

                    await handler(consumeResult.Message.Value, cancellationToken);

                    if (_options.EnableAutoCommit == false && consumeResult.Offset % _options.CommitOffsetPeriod == 0)
                    {
                        try
                        {
                            _consumer.Commit(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            _logger.LogError(e, "Commit Offset Error: {KafkaCommitError}", e.Error.Reason);
                        }
                    } else if (_options.EnableAutoCommit == false)
                    {
                        _consumer.StoreOffset(consumeResult);
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(
                        e,
                        "Consume error occurred. Topic: {Topic}, Error: {ConsumeError}",
                        e.ConsumerRecord.Topic,
                        e.Error.Reason);
                }
                catch (Exception e)
                {
                    _logger.LogError(
                        e,
                        "Unhandler exception on consuming. Topic: {Topic}, Error: {Error}",
                        _options.Topic,
                        e.Message);
                    throw;
                }
            }
        }
        finally
        {
            _logger.LogInformation("Closing consumer.");
            _consumer.Close();
        }
    }
}