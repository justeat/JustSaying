using System.Threading;
using System.Threading.Tasks;

namespace JustSaying.Messaging.Channels
{
    public interface IConsumerBus
    {
        void Start(CancellationToken stoppingToken);
        Task Completion { get; }
    }
}