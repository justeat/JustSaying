using Amazon.Auth.AccessControlPolicy;
using Amazon.Auth.AccessControlPolicy.ActionIdentifiers;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace JustSaying.AwsTools.MessageHandling
{
    public class SqsPolicy
    {
        private readonly string arn;
        private readonly IAmazonSQS client;
        private readonly string url;

        public SqsPolicy(string arn, string url, IAmazonSQS client)
        {
            this.arn = arn;
            this.url = url;
            this.client = client;
        }

        public void Set(string topicArn)
        {
            var topicArnWildcard = CreateTopicArnWildcard(topicArn);
            ActionIdentifier[] actions = { SQSActionIdentifiers.SendMessage };

            Policy sqsPolicy = new Policy()
                .WithStatements(new Statement(Statement.StatementEffect.Allow)
                    .WithPrincipals(Principal.AllUsers)
                    .WithResources(new Resource(arn))
                    .WithConditions(ConditionFactory.NewSourceArnCondition(topicArnWildcard))
                    .WithActionIdentifiers(actions));
            SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
            setQueueAttributesRequest.QueueUrl = url;
            setQueueAttributesRequest.Attributes["Policy"] = sqsPolicy.ToJson(); 
            client.SetQueueAttributes(setQueueAttributesRequest);
        }

        private string CreateTopicArnWildcard(string topicArn)
        {
            int index = topicArn.LastIndexOf(":");
            if (index > 0)
                topicArn = topicArn.Substring(0, index + 1);
            return topicArn + "*";
        }
    }
}