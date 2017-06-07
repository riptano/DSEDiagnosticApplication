using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net.Appender;
using log4net.Core;
using log4net.Util;

namespace DSEDiagnosticLogger
{
    public sealed class AsyncBufferingForwardingAppender : BufferingForwardingAppender
    {
        private readonly Common.Patterns.QueueProcessor<LoggingEvent[]> _logQueue = null;

        public AsyncBufferingForwardingAppender()
        {
            this._logQueue = new Common.Patterns.QueueProcessor<LoggingEvent[]>();
            this._logQueue.OnProcessMessageEvent += LogQueue_OnProcessMessageEvent;
            this._logQueue.OnExceptionEvent += LogQueue_OnExceptionEvent;
            this._logQueue.OnQueueThresholdEvent += LogQueue_OnQueueThresholdEvent;
            this._logQueue.Name = "Log4Net AsyncBufferingForwardingAppender";
            this._logQueue.StartQueue(true);
        }
        
        /// <summary>
        /// Activates the options for this appender.
        /// </summary>
        public override void ActivateOptions()
        {
            base.ActivateOptions();
        }

        /// <summary>
        /// Forwards the events to every configured appender.
        /// </summary>
        /// <param name="events">The events that need to be forwarded</param>
        protected override void SendBuffer(LoggingEvent[] events)
        {

            if (this._logQueue == null
                    || !this._logQueue.IsRunning
                    || this._logQueue.StopPending)
            {
                base.SendBuffer(events);
            }
            else
            {
                this._logQueue.Enqueue(events);
            }
        }

        /// <summary>
        /// Ensures that all pending logging events are flushed out before exiting.
        /// </summary>
        protected override void OnClose()
        {
            if (this._logQueue != null && !this._logQueue.Disposed)
            {
                if(this._logQueue.IsRunning && this._logQueue.Count > 0)
                {
                    this._logQueue.Suspend();
                    foreach (var logEvt in this._logQueue.ToArray())
                    {
                        base.SendBuffer(logEvt);
                    }
                }

                this._logQueue.Clear();
                this._logQueue.StopQueue();
                //this._logQueue.Dispose();
            }
            base.OnClose();
        }

        #region Queue Events
        private void LogQueue_OnExceptionEvent(Common.Patterns.QueueProcessor<LoggingEvent[]> sender, Common.Patterns.QueueProcessor<LoggingEvent[]>.ExceptionEventArgs exceptionEventArgs)
        {

            var loggerErrorEvent = new LoggingEvent(Logger.Instance.Log4NetInstance.Logger.GetType(),
                                                        Logger.Instance.Log4NetInstance.Logger.Repository,
                                                        this.GetType().Name,
                                                        Level.Error,
                                                        "Log4net AsyncBufferingForwardingAppender Queue received an Exception",
                                                        exceptionEventArgs.Exception);

            base.SendBuffer(new LoggingEvent[] { loggerErrorEvent });
            
            exceptionEventArgs.ThrowException = false;
        }

        private void LogQueue_OnProcessMessageEvent(Common.Patterns.QueueProcessor<LoggingEvent[]> sender, Common.Patterns.QueueProcessor<LoggingEvent[]>.MessageEventArgs processMessageArgs)
        {
            if (Logger.InvokeEvent(processMessageArgs.ProcessedMessage))
            {
                base.SendBuffer(processMessageArgs.ProcessedMessage);
            }
        }

        private void LogQueue_OnQueueThresholdEvent(Common.Patterns.QueueProcessor<LoggingEvent[]> sender, Common.Patterns.QueueProcessor<LoggingEvent[]>.ThresholdEventArgs thresholdLimitEventArgs)
        {
            var warning = new LoggingEvent(new LoggingEventData
            {
                Level = Level.Warn,
                LoggerName = this.GetType().Name,
                ThreadName = System.Threading.Thread.CurrentThread.ManagedThreadId.ToString(),
                TimeStampUtc = DateTime.UtcNow,
                Message = string.Format("Log4net AsyncBufferingForwardingAppender Queue Threhold warning. Current Queue size is {0:###,###,##0}; Threhold Limit is {1:###,###,##0}",
                                            thresholdLimitEventArgs.QueueCount,
                                            thresholdLimitEventArgs.ThresholdLimit)
            });

            base.SendBuffer(new LoggingEvent[] { warning });
        }

        #endregion


    }
}
