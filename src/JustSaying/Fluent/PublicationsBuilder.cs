using System;
using System.Collections.Generic;

namespace JustSaying.Fluent
{
    /// <summary>
    /// A class representing a builder for publications. This class cannot be inherited.
    /// </summary>
    public sealed class PublicationsBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PublicationsBuilder"/> class.
        /// </summary>
        /// <param name="parent">The <see cref="MessagingBusBuilder"/> that owns this instance.</param>
        internal PublicationsBuilder(MessagingBusBuilder parent)
        {
            Parent = parent;
        }

        /// <summary>
        /// Gets the parent of this builder.
        /// </summary>
        internal MessagingBusBuilder Parent { get; }

        /// <summary>
        /// Gets the configured publication builders.
        /// </summary>
        private IList<IPublicationBuilder> Publications { get; } = new List<IPublicationBuilder>();

        /// <summary>
        /// Configures a publisher for a queue.
        /// </summary>
        /// <typeparam name="T">The type of the message to publish.</typeparam>
        /// <returns>
        /// The current <see cref="PublicationsBuilder"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public PublicationsBuilder WithQueue<T>()
            where T : class
        {
            Publications.Add(new QueuePublicationBuilder<T>());
            return this;
        }

        /// <summary>
        /// Configures a publisher for a queue.
        /// </summary>
        /// <typeparam name="T">The type of the message to publish.</typeparam>
        /// <param name="name">The name to use for the queue.</param>
        /// <returns>
        /// The current <see cref="PublicationsBuilder"/>.
        /// </returns>
        public PublicationsBuilder WithQueue<T>(string name)
            where T : class
        {
            return WithQueue<T>((options) => options.WithWriteConfiguration((r) => r.WithQueueName(name)));
        }

        /// <summary>
        /// Configures a publisher for a queue.
        /// </summary>
        /// <typeparam name="T">The type of the message to publish.</typeparam>
        /// <param name="configure">A delegate to a method to use to configure a queue.</param>
        /// <returns>
        /// The current <see cref="PublicationsBuilder"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public PublicationsBuilder WithQueue<T>(Action<QueuePublicationBuilder<T>> configure)
            where T : class
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            var builder = new QueuePublicationBuilder<T>();

            configure(builder);

            Publications.Add(builder);

            return this;
        }

        /// <summary>
        /// Configures a publisher for a topic.
        /// </summary>
        /// <typeparam name="T">The type of the message to publish.</typeparam>
        /// <returns>
        /// The current <see cref="PublicationsBuilder"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public PublicationsBuilder WithTopic<T>()
            where T : class
        {
            Publications.Add(new TopicPublicationBuilder<T>());
            return this;
        }

        /// <summary>
        /// Configures a publisher for a topic.
        /// </summary>
        /// <typeparam name="T">The type of the message to publish.</typeparam>
        /// <param name="configure">A delegate to a method to use to configure a topic.</param>
        /// <returns>
        /// The current <see cref="PublicationsBuilder"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public PublicationsBuilder WithTopic<T>(Action<TopicPublicationBuilder<T>> configure)
            where T : class
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            var builder = new TopicPublicationBuilder<T>();

            configure(builder);

            Publications.Add(builder);

            return this;
        }

        /// <summary>
        /// Configures the publications for the <see cref="JustSayingFluently"/>.
        /// </summary>
        /// <param name="bus">The <see cref="JustSayingFluently"/> to configure publications for.</param>
        internal void Configure(JustSayingFluently bus)
        {
            foreach (IPublicationBuilder builder in Publications)
            {
                builder.Configure(bus);
            }
        }
    }
}
