﻿using System;

namespace JustSaying.AwsTools.MessageHandling
{
    [Serializable]
    public class PublishException : Exception
    {
        public PublishException()
        {
        }

        public PublishException(string message) : base(message)
        {
        }
        public PublishException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
