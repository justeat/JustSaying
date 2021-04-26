using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.Messaging.Channels.Context;

namespace JustSaying.AwsTools.MessageHandling
{
    internal class SqsQueueReader
    {
        private readonly ISqsQueue _sqsQueue;

        internal SqsQueueReader(ISqsQueue sqsQueue)
        {
            _sqsQueue = sqsQueue;
        }

        internal string QueueName => _sqsQueue.QueueName;

        internal string RegionSystemName => _sqsQueue.RegionSystemName;

        internal Uri Uri => _sqsQueue.Uri;

        internal IQueueMessageContext ToMessageContext(Message message)
        {
            return new QueueMessageContext(message, this);
        }

        internal async Task<IList<Message>> GetMessagesAsync(
            int maximumCount,
            TimeSpan waitTime,
            IEnumerable<string> requestMessageAttributeNames,
            CancellationToken cancellationToken)
        {
            var request = new ReceiveMessageRequest
            {
                QueueUrl = _sqsQueue.Uri.AbsoluteUri,
                MaxNumberOfMessages = maximumCount,
                WaitTimeSeconds = (int)waitTime.TotalSeconds,
                AttributeNames = requestMessageAttributeNames.ToList()
            };

            ReceiveMessageResponse sqsMessageResponse =
                await _sqsQueue.Client.ReceiveMessageAsync(request, cancellationToken).ConfigureAwait(false);

            return sqsMessageResponse?.Messages;
        }

        internal Task DeleteMessageAsync(
            string receiptHandle,
            CancellationToken cancellationToken)
        {
            var deleteRequest = new DeleteMessageRequest
            {
                QueueUrl = _sqsQueue.Uri.AbsoluteUri,
                ReceiptHandle = receiptHandle,
            };

            return _sqsQueue.Client.DeleteMessageAsync(deleteRequest, cancellationToken);
        }

        internal Task ChangeMessageVisibilityAsync(
            string receiptHandle,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            var visibilityRequest = new ChangeMessageVisibilityRequest
            {
                QueueUrl = _sqsQueue.Uri.ToString(),
                ReceiptHandle = receiptHandle,
                VisibilityTimeout = (int)timeout.TotalSeconds,
            };

            return _sqsQueue.Client.ChangeMessageVisibilityAsync(visibilityRequest, cancellationToken);
        }
    }
}
