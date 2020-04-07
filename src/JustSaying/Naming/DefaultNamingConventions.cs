using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using JustSaying.AwsTools.QueueCreation;

namespace JustSaying.Naming
{
    /// <summary>
    /// A class representing the default implementations of <see cref="ITopicNamingConvention"/> and <see cref="IQueueNamingConvention"/>. This is used when no custom implementation is provided.
    /// </summary>
    public class DefaultNamingConventions : ITopicNamingConvention, IQueueNamingConvention
    {
        private const int MaxTopicNameLength = 256;
        private const int MaxQueueNameLength = 80;

        private static readonly HashSet<Type> TypesToMapAutomatically = new HashSet<Type>
        {
            typeof(string),
            typeof(object),
            typeof(bool),
            typeof(byte),
            typeof(char),
            typeof(decimal),
            typeof(double),
            typeof(short),
            typeof(int),
            typeof(long),
            typeof(sbyte),
            typeof(float),
            typeof(ushort),
            typeof(uint),
            typeof(ulong),
            typeof(void),
            typeof(TimeSpan),
            typeof(DateTime),
            typeof(DateTimeOffset)
        };

        public virtual string TopicName<T>(string nameOverride) => CreateResourceName(typeof(T), MaxTopicNameLength);

        public virtual string QueueName<T>(string nameOverride) => CreateResourceName(typeof(T), MaxQueueNameLength);

        private static string CreateResourceName(Type type, int maximumLength)
        {
            var name = Regex.Replace(GetTypeFriendlyName(type), "[^a-zA-Z0-9_-]", string.Empty);

            return name.Length <= maximumLength ? name.ToLowerInvariant() : name.Substring(0, maximumLength);
        }

        private static string GetTypeFriendlyName(Type type)
        {
            var friendlyName = type.Name.ToLowerInvariant();

            if (TypesToMapAutomatically.Contains(type))
            {
                return friendlyName;
            }

            if (type.GetTypeInfo().IsGenericType)
            {
                var indexOfBacktick = friendlyName.IndexOf('`');

                if (indexOfBacktick > 0)
                {
                    friendlyName = friendlyName.Remove(indexOfBacktick);
                }

                friendlyName += string.Join("_", type.GenericTypeArguments.Select(GetTypeFriendlyName));
            }

            if (type.IsArray)
            {
                return GetTypeFriendlyName(type.GetElementType()) + "_";
            }

            return friendlyName;
        }
    }
}
