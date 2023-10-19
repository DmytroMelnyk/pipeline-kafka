using NUnit.Framework;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.Pipeline;

namespace Pipeline.Kafka.Tests;

[TestFixture]
public class ChainOfResponsibilityTests
{
    [Test]
    public async Task RemoveAtWhenCalledOnceExpectedMinusOneLementInBatch()
    {
        var arr = new List<string> { "str1", "str2" };
        var chain = new Chain(Enumerable.Repeat(new BatchLink(), 1));
        await chain.ExecuteAsync(arr.ToBatch(), default);
        Assert.That(chain.Messages, Has.Count.EqualTo(arr.Count - 1));
    }
}

public class BatchLink : IBatchPipelineLink<object>
{
    public Task RunAsync(IBatch<object> message, Func<Task> next, CancellationToken cancellationToken)
    {
        message.RemoveAt(0);
        return next();
    }
}

public class Chain : IChainOfResponsibility<IBatch<string>>
{
    private readonly IEnumerable<IBatchPipelineLink<string>> _links;

    public IBatch<string>? Messages
    {
        get; set;
    }

    public Chain(IEnumerable<IBatchPipelineLink<string>> links) => _links = links;

    public Task ExecuteAsync(IBatch<string> batch, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<IBatch<string>>) this).ExecuteAsyncImpl(_links.GetEnumerator(), batch, cancellationToken);

    Task IChainOfResponsibility<IBatch<string>>.ExecuteHandlerAsync(IBatch<string> message, CancellationToken cancellationToken)
    {
        Messages = message;
        return Task.CompletedTask;
    }
}
