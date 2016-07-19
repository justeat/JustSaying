using System.Collections.Generic;

namespace JustSaying.AwsTools.QueueCreation
{
    public class RegionResourceCache<T> : Dictionary<string, Dictionary<string, T>>, IRegionResourceCache<T>
    {
        public T TryGetFromCache(string region, string key)
        {
            if (! ContainsKey(region))
            {
                return default(T);
            }

            var regionDict = this[region];
            if (! regionDict.ContainsKey(key))
            {
                return default(T);
            }

            return regionDict[key];
        }

        public void AddToCache(string region, string key, T value)
        {
            if (!ContainsKey(region))
            {
                this[region] = new Dictionary<string, T>();
            }
            var regionDict = this[region];
            regionDict[key] = value;
        }
    }
}