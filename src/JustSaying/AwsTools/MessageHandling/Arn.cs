namespace JustSaying.AwsTools.MessageHandling
{
    /// <summary>
    /// Represents an Arn and allows access to its elements
    /// </summary>
    internal struct Arn
    {
        private const int ARN = 0;
        private const int PARTITION = 1;
        private const int SERVICE = 2;
        private const int REGION = 3;
        private const int ACCOUNT = 4;
        private const int RESOURCE = 5;
        private readonly string _arn;
        private string[] _decomposedArn;

        public string Resource => _decomposedArn[RESOURCE];

        public Arn(string arn)
        {
            _arn = arn;
            _decomposedArn = _arn.Split(':');
        }

    }
}
