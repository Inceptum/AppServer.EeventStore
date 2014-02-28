using System;
using System.Linq;
using System.Threading;

using IEventStoreLogger = EventStore.Common.Log.ILogger;
using ICastleLogger = Castle.Core.Logging.ILogger;

namespace Inceptum.Applications.EventStoreNode.Common
{
    public class CastleToEventStoreLoggerAdapter : IEventStoreLogger
    {
        private readonly ICastleLogger m_Logger;

        public CastleToEventStoreLoggerAdapter(ICastleLogger logger)
        {
            m_Logger = logger;
        }

        public void Flush(TimeSpan? maxTimeToWait = null)
        {
            //TODO[MT]: revise log flush
            FlushLog(maxTimeToWait);
        }

        public void Fatal(string text)
        {
            m_Logger.Fatal(text);
        }

        public void Error(string text)
        {
            m_Logger.Error(text);
        }

        public void Info(string text)
        {
            m_Logger.Info(text);
        }

        public void Debug(string text)
        {
            m_Logger.Debug(text);
        }

        public void Trace(string text)
        {
            m_Logger.Debug(text);
        }
        
        public void Fatal(string format, params object[] args)
        {
            m_Logger.FatalFormat(format, args);
        }

        public void Error(string format, params object[] args)
        {
            m_Logger.ErrorFormat(format, args);
        }

        public void Info(string format, params object[] args)
        {
            m_Logger.InfoFormat(format, args);
        }

        public void Debug(string format, params object[] args)
        {
            m_Logger.DebugFormat(format, args);
        }

        public void Trace(string format, params object[] args)
        {
            m_Logger.DebugFormat(format, args);
        }


        public void FatalException(Exception exc, string message)
        {
            m_Logger.Fatal(message, exc);
        }

        public void ErrorException(Exception exc, string message)
        {
            m_Logger.Error(message, exc);
        }

        public void InfoException(Exception exc, string message)
        {
            m_Logger.Info(message, exc);
        }

        public void DebugException(Exception exc, string message)
        {
            m_Logger.Debug(message, exc);
        }

        public void TraceException(Exception exc, string message)
        {
            m_Logger.Debug(message, exc);
        }

        public void FatalException(Exception exc, string format, params object[] args)
        {
            m_Logger.Fatal(string.Format(format, args), exc);
        }

        public void ErrorException(Exception exc, string format, params object[] args)
        {
            m_Logger.Error(string.Format(format, args), exc);
        }

        public void InfoException(Exception exc, string format, params object[] args)
        {
            m_Logger.Info(string.Format(format, args), exc);
        }

        public void DebugException(Exception exc, string format, params object[] args)
        {
            m_Logger.Debug(string.Format(format, args), exc);
        }

        public void TraceException(Exception exc, string format, params object[] args)
        {
            m_Logger.Debug(string.Format(format, args), exc);
        }

        public static void FlushLog(TimeSpan? maxTimeToWait = null)
        {
            var config = NLog.LogManager.Configuration;
            if (config == null)
                return;
            var asyncs = config.AllTargets.OfType<NLog.Targets.Wrappers.AsyncTargetWrapper>().ToArray();
            var countdown = new CountdownEvent(asyncs.Length);
            foreach (var wrapper in asyncs)
            {
                wrapper.Flush(x => countdown.Signal());
            }
            countdown.Wait(maxTimeToWait ?? TimeSpan.FromMilliseconds(500));
        }
    }
}