using Messagefy.Abstractions.MessageBus;
using Messagefy.Kafka.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Messagefy.Kafka;

/// <summary>
/// Contains extension methods for adding Kafka producers and consumers to the service collection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds a Kafka producer to the service collection.
    /// </summary>
    /// <typeparam name="TMessage">The type of message that the producer will handle.</typeparam>
    /// <param name="services">The service collection to which the producer will be added.</param>
    /// <param name="options">The configuration options for the Kafka producer.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddKafkaProducer<TMessage>(this IServiceCollection services, KafkaProducerOptions options) 
        where TMessage : class
    {
        services.AddSingleton<IMessageBusProducer<TMessage>>(new KafkaProducer<TMessage>(options));

        return services;
    }

    /// <summary>
    /// Adds a Kafka consumer to the service collection.
    /// </summary>
    /// <typeparam name="TMessage">The type of message that the consumer will handle.</typeparam>
    /// <param name="services">The service collection to which the consumer will be added.</param>
    /// <param name="options">The configuration options for the Kafka consumer.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddKafkaConsumer<TMessage>(this IServiceCollection services, KafkaConsumerOptions options)
        where TMessage : class
    {
        services.AddSingleton<IMessageBusConsumer<TMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaConsumer<TMessage>>>();

            return new KafkaConsumer<TMessage>(options, logger);
        });

        return services;
    }
}