namespace Pipeline.Kafka.Pipeline;

internal interface IChainOfResponsibility<T>
{
    public async Task ExecuteAsyncImpl(IEnumerator<IPipelineLink<T>> chain, T message, CancellationToken cancellationToken)
    {
        if (chain.MoveNext())
        {
            await chain.Current.RunAsync(message, () => ExecuteAsyncImpl(chain, message, cancellationToken), cancellationToken);
        }
        else
        {
            await ExecuteHandlerAsync(message, cancellationToken);
        }
    }

    Task ExecuteHandlerAsync(T message, CancellationToken cancellationToken);
}
