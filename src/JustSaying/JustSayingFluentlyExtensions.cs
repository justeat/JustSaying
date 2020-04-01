using System.Collections.Generic;
using JustSaying.AwsTools;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging.MessageSerialization;
using JustSaying.Messaging.Monitoring;
using JustSaying.Naming;

namespace JustSaying
{
    public static class JustSayingFluentlyExtensions
    {
        public static JustSayingFluentlyDependencies WithMessageSubjectProvider(this JustSayingFluentlyDependencies dependencies,
            IMessageSubjectProvider messageSubjectProvider)
        {
            dependencies.MessageSubjectProvider = messageSubjectProvider;
            return dependencies;
        }

        public static JustSayingFluentlyDependencies UsingTopicNameConvention(
            this JustSayingFluentlyDependencies dependencies,
            ITopicNamingConvention topicNamingConvention)
        {
            dependencies.TopicNamingConvention = topicNamingConvention;
            return dependencies;
        }

        public static JustSayingFluentlyDependencies UsingQueueNameConvention(
            this JustSayingFluentlyDependencies dependencies,
            IQueueNamingConvention queueNamingConvention)
        {
            dependencies.QueueNamingConvention = queueNamingConvention;
            return dependencies;
        }

        /// <summary>
        /// Note: using this message subject provider may cause incompatibility with applications using prior versions of Just Saying
        /// </summary>
        public static JustSayingFluentlyDependencies WithGenericMessageSubjectProvider(this JustSayingFluentlyDependencies dependencies) =>
            dependencies.WithMessageSubjectProvider(new GenericMessageSubjectProvider());

        public static IMayWantOptionalSettings InRegion(this JustSayingFluentlyDependencies dependencies, string region) => InRegions(dependencies, region);

        public static IMayWantOptionalSettings InRegions(this JustSayingFluentlyDependencies dependencies, params string[] regions) => InRegions(dependencies, regions as IEnumerable<string>);

        public static IMayWantOptionalSettings InRegions(this JustSayingFluentlyDependencies dependencies, IEnumerable<string> regions)
        {
            var config = new MessagingConfig();

            if (dependencies.MessageSubjectProvider != null)
                config.MessageSubjectProvider = dependencies.MessageSubjectProvider;

            if (dependencies.TopicNamingConvention != null)
                config.TopicNamingConvention = dependencies.TopicNamingConvention;

            if (dependencies.QueueNamingConvention != null)
                config.QueueNamingConvention = dependencies.QueueNamingConvention;

            if (regions != null)
                foreach (var region in regions)
                {
                    config.Regions.Add(region);
                }

            config.Validate();

            var serializationFactory = dependencies.SerializationFactory ?? new NewtonsoftSerializationFactory();

            var messageSerializationRegister = new MessageSerializationRegister(
                config.MessageSubjectProvider,
                serializationFactory);
            var justSayingBus = new JustSayingBus(config, messageSerializationRegister, dependencies.LoggerFactory);

            var awsClientFactoryProxy = new AwsClientFactoryProxy(() => CreateMeABus.DefaultClientFactory());

            var amazonQueueCreator = new AmazonQueueCreator(awsClientFactoryProxy, dependencies.LoggerFactory);

            var bus = new JustSayingFluently(justSayingBus,
                amazonQueueCreator,
                awsClientFactoryProxy,
                serializationFactory,
                dependencies.LoggerFactory);

            bus
                .WithMonitoring(new NullOpMessageMonitor());

            return bus;
        }

        public static IFluentSubscription IntoDefaultQueue(this ISubscriberIntoQueue subscriber)
        {
            return subscriber.IntoQueue(string.Empty);
        }
    }
}
