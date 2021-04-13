using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageSerialization;
using Microsoft.Extensions.Logging;
using Message = JustSaying.Models.Message;
using MessageAttributeValue = Amazon.SQS.Model.MessageAttributeValue;

namespace JustSaying.AwsTools.MessageHandling
{
    [Obsolete("SqsQueueBase and related classes are not intended for general usage and may be removed in a future major release")]
    public class SqsPublisher : SqsQueueByName, IMessagePublisher
    {
        private readonly IAmazonSQS _client;
        private readonly IMessageSerializationRegister _serializationRegister;
        public Action<MessageResponse, Message> MessageResponseLogger { get; set; }

        public SqsPublisher(
            RegionEndpoint region,
            string queueName,
            IAmazonSQS client,
            int retryCountBeforeSendingToErrorQueue,
            IMessageSerializationRegister serializationRegister,
            ILoggerFactory loggerFactory)
            : base(region, queueName, retryCountBeforeSendingToErrorQueue, client, loggerFactory)
        {
            _client = client;
            _serializationRegister = serializationRegister;
        }

        // TODO: This type shouldn't be an IMessagePublisher
        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public async Task PublishAsync(Message message, CancellationToken cancellationToken)
            => await PublishAsync(message, null, cancellationToken).ConfigureAwait(false);

        public async Task PublishAsync(Message message, PublishMetadata metadata, CancellationToken cancellationToken)
        {
            var request = BuildSendMessageRequest(message, metadata);
            SendMessageResponse response;
            try
            {
                response = await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonServiceException ex)
            {
                throw new PublishException(
                    $"Failed to publish message to SQS. {nameof(request.QueueUrl)}: {request.QueueUrl},{nameof(request.MessageBody)}: {request.MessageBody}",
                    ex);
            }

            using (Logger.BeginScope(new[]
            {
                new KeyValuePair<string, object>("AwsRequestId", response?.MessageId)
            }))
            {
                Logger.LogInformation(
                    "Published message {MessageId} of type {MessageType} to {DestinationType} '{MessageDestination}'.",
                    message.Id,
                    message.GetType().Name,
                    "Queue",
                    request.QueueUrl);
            }

            if (MessageResponseLogger != null)
            {
                var responseData = new MessageResponse
                {
                    HttpStatusCode = response?.HttpStatusCode,
                    MessageId = response?.MessageId,
                    ResponseMetadata = response?.ResponseMetadata
                };
                MessageResponseLogger.Invoke(responseData, message);
            }
        }

        private SendMessageRequest BuildSendMessageRequest(Message message, PublishMetadata metadata)
        {
            var request = new SendMessageRequest
            {
                MessageBody = GetMessageInContext(message),
                QueueUrl = Uri?.AbsoluteUri,
            };

            if (metadata?.Delay != null)
            {
                request.DelaySeconds = (int)metadata.Delay.Value.TotalSeconds;
            }
            return request;
        }

        public string GetMessageInContext(Message message) => _serializationRegister.Serialize(message, serializeForSnsPublishing: false);
    }
}
