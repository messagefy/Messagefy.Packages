namespace Messagefy.Abstractions.MessageBus;

/// <summary>
/// Represents a consumer for a message bus that handles messages of type <typeparamref name="TMessage"/>.
/// </summary>
/// <typeparam name="TMessage">The type of message to be consumed.</typeparam>
public interface IMessageBusConsumer<TMessage> : IAsyncDisposable where TMessage : class
{
    Task ConsumeAsync(
        Func<TMessage, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default);
}
