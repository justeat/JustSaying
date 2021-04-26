using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using JustSaying.AwsTools;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Models;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent
{
    public abstract class IntegrationTestBase
    {
        protected IntegrationTestBase(ITestOutputHelper outputHelper)
        {
            OutputHelper = new CapturingTestOutputHelper(outputHelper);
        }

        protected virtual string AccessKeyId { get; } = "accessKeyId";

        protected virtual string SecretAccessKey { get; } = "secretAccessKey";

        protected virtual string SessionToken { get; } = "token";

        protected CapturingTestOutputHelper OutputHelper { get; }

        protected virtual string RegionName => Region.SystemName;

        protected virtual Amazon.RegionEndpoint Region => TestEnvironment.Region;

        protected virtual Uri ServiceUri => TestEnvironment.SimulatorUrl;

        protected virtual bool IsSimulator => TestEnvironment.IsSimulatorConfigured;

        protected virtual TimeSpan Timeout => TimeSpan.FromSeconds(Debugger.IsAttached ? 60 : 20);

        protected virtual string UniqueName { get; } = $"{Guid.NewGuid():N}-integration-tests";

        protected IServiceCollection GivenJustSaying(LogLevel? levelOverride = null)
            => Given((_) => { }, levelOverride);

        protected IServiceCollection Given(
            Action<MessagingBusBuilder> configure,
            LogLevel? levelOverride = null)
            => Given((builder, _) => configure(builder), levelOverride);

        protected IServiceCollection Given(
            Action<MessagingBusBuilder, IServiceProvider> configure,
            LogLevel? levelOverride = null)
        {
            return new ServiceCollection()
                .AddLogging((p) => p.AddXUnit(OutputHelper, o => o.IncludeScopes = true).SetMinimumLevel(levelOverride ?? LogLevel.Debug))
                .AddJustSaying(
                    (builder, serviceProvider) =>
                    {
                        builder.Messaging((options) => options.WithRegion(RegionName))
                            .Client((options) =>
                            {
                                options.WithSessionCredentials(AccessKeyId, SecretAccessKey, SessionToken)
                                    .WithServiceUri(ServiceUri);
                            });

                        configure(builder, serviceProvider);
                    });
        }

        protected virtual IAwsClientFactory CreateClientFactory()
        {
            var credentials = new SessionAWSCredentials(AccessKeyId, SecretAccessKey, SessionToken);
            return new DefaultAwsClientFactory(credentials) { ServiceUri = ServiceUri };
        }

        protected IHandlerAsync<T> CreateHandler<T>(TaskCompletionSource<object> completionSource)
            where T : Message
        {
            IHandlerAsync<T> handler = Substitute.For<IHandlerAsync<T>>();

            handler.Handle(Arg.Any<T>())
                .Returns(true)
                .AndDoes((_) => completionSource.TrySetResult(null));

            return handler;
        }

        protected async Task WhenAsync(
            IServiceCollection services,
            Func<IMessagePublisher, IMessagingBus, CancellationToken, Task> action)
            => await WhenAsync(services, async (p, b, _, c) => await action(p, b, c));

        protected async Task WhenAsync(
            IServiceCollection services,
            Func<IMessagePublisher, IMessagingBus, IServiceProvider, CancellationToken, Task> action)
        {
            IServiceProvider serviceProvider = services.BuildServiceProvider();

            IMessagePublisher publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            IMessagingBus listener = serviceProvider.GetRequiredService<IMessagingBus>();

            await RunActionWithTimeout(async cancellationToken =>
                await action(publisher, listener, serviceProvider, cancellationToken)
                    .ConfigureAwait(false));
        }

        protected async Task RunActionWithTimeout(Func<CancellationToken, Task> action)
        {
            // See https://speakerdeck.com/davidfowl/scaling-asp-dot-net-core-applications?slide=28
            using (var cts = new CancellationTokenSource())
            {
                var delayTask = Task.Delay(Timeout, cts.Token);
                var actionTask = action(cts.Token);

                var resultTask = await Task.WhenAny(actionTask, delayTask)
                    .ConfigureAwait(false);

                if (resultTask == delayTask)
                {
                    throw new TimeoutException(
                        $"The tested action took longer than the timeout of {Timeout} to complete.");
                }
                else
                {
                    cts.Cancel();
                }

                await actionTask;
            }
        }

        public class CapturingTestOutputHelper : ITestOutputHelper
        {
            private readonly ITestOutputHelper _inner;
            private readonly StringBuilder _sb;

            public string Output => _sb.ToString();

            public CapturingTestOutputHelper(ITestOutputHelper inner)
            {
                _inner = inner;
                _sb = new StringBuilder();
            }

            public void WriteLine(string message)
            {
                _sb.AppendLine(message);
                _inner.WriteLine(message);
            }

            public void WriteLine(string format, params object[] args)
            {
                _sb.AppendLine(string.Format(format, args));
                _inner.WriteLine(format, args);
            }
        }
    }
}
