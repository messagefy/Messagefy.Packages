namespace Messagefy.Abstractions.MessageBus;

/// <summary>
/// Defines the contract for a message bus producer that can produce messages of type <typeparamref name="TMessage"/>.
/// </summary>
/// <typeparam name="TMessage">The type of message that the producer can produce.</typeparam>
public interface IMessageBusProducer<TMessage> : IAsyncDisposable
    where TMessage : class
{
    Task ProduceAsync(TMessage message, CancellationToken cancellationToken = default);
}