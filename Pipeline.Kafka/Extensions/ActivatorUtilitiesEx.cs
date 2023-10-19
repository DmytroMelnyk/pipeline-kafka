using Microsoft.Extensions.DependencyInjection;

namespace Pipeline.Kafka.Extensions;

public static class ActivatorUtilitiesEx
{
    public static Func<IServiceProvider, T1, TResult> CreateFactory<T1, TResult>()
    {
        var factory = ActivatorUtilities.CreateFactory(typeof(TResult), new[]
        {
        typeof(T1)
    });

        return (sp, arg1) => (TResult) factory(sp, new object?[]
        {
        arg1
        });
    }

    public static Func<IServiceProvider, T1, T2, TResult> CreateFactory<T1, T2, TResult>()
    {
        var factory = ActivatorUtilities.CreateFactory(typeof(TResult), new[]
        {
        typeof(T1),
        typeof(T2)
    });

        return (sp, arg1, arg2) => (TResult) factory(sp, new object?[]
        {
        arg1,
        arg2
        });
    }
}
