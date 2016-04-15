using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JustSaying.Models;
using JustSaying.TestingFramework;

namespace JustSaying.IntegrationTests.JustSayingFluently
{
    public class Future<TMessage> where TMessage : Message
    {
        private readonly TaskCompletionSource<object> _doneSignal = new TaskCompletionSource<object>();

        private readonly Action _action;
        private readonly List<TMessage> _messages = new List<TMessage>();

        public Future(): this(null)
        {
        }

        public Future(Action action)
        {
            _action = action;
        }

        public void Complete(TMessage message)
        {
            try
            {
                Value = message;
                _messages.Add(message);
                if (_action != null)
                {
                    _action();
                }
            }
            finally
            {
                Tasks.DelaySendDone(_doneSignal);
            }
        }

        public Task DoneSignal
        {
            get { return _doneSignal.Task; }
        }

        public int MessageCount
        {
            get { return _messages.Count; }
        }

        public Exception RecordedException { get; set; }

        public TMessage Value { get; set; }

        public bool HasReceived(TMessage message)
        {
            return _messages.Any(m => m.Id == message.Id);
        }
    }
}