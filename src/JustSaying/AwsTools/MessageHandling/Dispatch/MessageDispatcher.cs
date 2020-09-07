using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.SQS;
using JustSaying.Messaging.Channels.Context;
using JustSaying.Messaging.Interrogation;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageProcessingStrategies;
using JustSaying.Messaging.MessageSerialization;
using JustSaying.Messaging.Monitoring;
using Microsoft.Extensions.Logging;
using Message = JustSaying.Models.Message;

namespace JustSaying.AwsTools.MessageHandling.Dispatch
{
    public class MessageDispatcher : IMessageDispatcher
    {
        private readonly IMessageSerializationRegister _serializationRegister;
        private readonly IMessageMonitor _messagingMonitor;
        private readonly HandlerMap _handlerMap;
        private readonly IMessageBackoffStrategy _messageBackoffStrategy;
        private readonly IMessageContextAccessor _messageContextAccessor;

        private static ILogger _logger;

        public MessageDispatcher(
            IMessageSerializationRegister serializationRegister,
            IMessageMonitor messagingMonitor,
            HandlerMap handlerMap,
            ILoggerFactory loggerFactory,
            IMessageBackoffStrategy messageBackoffStrategy,
            IMessageContextAccessor messageContextAccessor)
        {
            _serializationRegister = serializationRegister;
            _messagingMonitor = messagingMonitor;
            _handlerMap = handlerMap;
            _logger = loggerFactory.CreateLogger("JustSaying");
            _messageBackoffStrategy = messageBackoffStrategy;
            _messageContextAccessor = messageContextAccessor;
        }

        public async Task DispatchMessageAsync(
            IQueueMessageContext messageContext,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            (bool success, Message typedMessage, MessageAttributes attributes) =
                await DeserializeMessage(messageContext, cancellationToken).ConfigureAwait(false);

            if (!success)
            {
                return;
            }

            var handlingSucceeded = false;
            Exception lastException = null;

            try
            {
                if (typedMessage != null)
                {
                    _messageContextAccessor.MessageContext =
                        new MessageContext(messageContext.Message, messageContext.QueueUri, attributes);

                    handlingSucceeded = await CallMessageHandler(messageContext.QueueName, typedMessage)
                        .ConfigureAwait(false);
                }

                if (handlingSucceeded)
                {
                    await messageContext.DeleteMessageFromQueueAsync(cancellationToken).ConfigureAwait(false);
                }
            }
#pragma warning disable CA1031
            catch (Exception ex)
#pragma warning restore CA1031
            {
                _logger.LogError(
                    ex,
                    "Error handling message with Id '{MessageId}' and body '{MessageBody}'.",
                    messageContext.Message.MessageId,
                    messageContext.Message.Body);

                if (typedMessage != null)
                {
                    _messagingMonitor.HandleException(typedMessage.GetType());
                }

                _messagingMonitor.HandleError(ex, messageContext.Message);

                lastException = ex;
            }
            finally
            {
                try
                {
                    if (!handlingSucceeded && _messageBackoffStrategy != null)
                    {
                        await UpdateMessageVisibilityTimeout(messageContext,
                            typedMessage,
                            lastException,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _messageContextAccessor.MessageContext = null;
                }
            }
        }

        private async Task<(bool success, Message typedMessage, MessageAttributes attributes)>
            DeserializeMessage(IQueueMessageContext messageContext, CancellationToken cancellationToken)
        {
            try
            {
                var messageWithAttributes = _serializationRegister.DeserializeMessage(messageContext.Message.Body);
                return (true, messageWithAttributes.Message, messageWithAttributes.MessageAttributes);
            }
            catch (MessageFormatNotSupportedException ex)
            {
                _logger.LogTrace(ex,
                    "Could not handle message with Id '{MessageId}' because a deserializer for the content is not configured. Message body: '{MessageBody}'.",
                    messageContext.Message.MessageId,
                    messageContext.Message.Body);

                await messageContext.DeleteMessageFromQueueAsync(cancellationToken).ConfigureAwait(false);
                _messagingMonitor.HandleError(ex, messageContext.Message);

                return (false, null, null);
            }
#pragma warning disable CA1031
            catch (Exception ex)
#pragma warning restore CA1031
            {
                _logger.LogError(
                    ex,
                    "Error deserializing message with Id '{MessageId}' and body '{MessageBody}'.",
                    messageContext.Message.MessageId,
                    messageContext.Message.Body);

                _messagingMonitor.HandleError(ex, messageContext.Message);

                return (false, null, null);
            }
        }

        private async Task<bool> CallMessageHandler(string queueName, Message message)
        {
            var messageType = message.GetType();

            var handler = _handlerMap.Get(queueName, messageType);

            if (handler == null)
            {
                _logger.LogError(
                    "Failed to dispatch. Handler for message of type '{MessageTypeName}' not found in handler map.",
                    message.GetType().FullName);
                return false;
            }

            var watch = System.Diagnostics.Stopwatch.StartNew();

            using (_messagingMonitor.MeasureHandler())
            {
                bool handlerSucceeded = false;
                try
                {
                    handlerSucceeded = await handler(message).ConfigureAwait(false);
                }
                finally
                {
                    watch.Stop();

                    var logMessage =
                        "{Status} handling message with Id '{MessageId}' of type {MessageType} in {TimeToHandle}ms.";
                    if (handlerSucceeded)
                    {
                        _logger.LogInformation(logMessage,
                            "Succeeded",
                            message.Id,
                            messageType,
                            watch.ElapsedMilliseconds);
                    }
                    else
                    {
                        _logger.LogWarning(logMessage,
                            "Failed",
                            message.Id,
                            messageType,
                            watch.ElapsedMilliseconds);
                    }
                }

                return handlerSucceeded;
            }
        }

        private async Task UpdateMessageVisibilityTimeout(
            IQueueMessageContext messageContext,
            Message typedMessage,
            Exception lastException,
            CancellationToken cancellationToken)
        {
            if (TryGetApproxReceiveCount(messageContext.Message.Attributes, out int approxReceiveCount))
            {
                var visibilityTimeout =
                    _messageBackoffStrategy.GetBackoffDuration(typedMessage,
                        approxReceiveCount,
                        lastException);

                try
                {
                    await messageContext.ChangeMessageVisibilityAsync(visibilityTimeout, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (AmazonServiceException ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to update message visibility timeout by {VisibilityTimeout} seconds for message with receipt handle '{ReceiptHandle}'.",
                        visibilityTimeout,
                        messageContext.Message.ReceiptHandle);

                    _messagingMonitor.HandleError(ex, messageContext.Message);
                }
            }
        }

        private static bool TryGetApproxReceiveCount(
            IDictionary<string, string> attributes,
            out int approxReceiveCount)
        {
            approxReceiveCount = 0;

            return attributes.TryGetValue(MessageSystemAttributeName.ApproximateReceiveCount,
                    out string rawApproxReceiveCount) &&
                int.TryParse(rawApproxReceiveCount,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out approxReceiveCount);
        }
    }
}
