﻿using NUnitLite;
using System.Reflection;

namespace JustSaying.AwsTools.IntegrationTests
{
    class Program
    {
        public static int Main(string[] args)
        {
            return new AutoRun(typeof(Program).GetTypeInfo().Assembly).Execute(args);
        }
    }

}
