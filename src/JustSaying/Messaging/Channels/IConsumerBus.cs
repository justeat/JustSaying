using System.Threading;
using System.Threading.Tasks;

namespace JustSaying.Messaging.Channels
{
    public interface IConsumerBus
    {
        Task Run(CancellationToken stoppingToken);
    }
}
