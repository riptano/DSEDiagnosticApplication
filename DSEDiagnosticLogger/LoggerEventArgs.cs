using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticLogger
{

    [System.Diagnostics.DebuggerNonUserCode]
    public sealed class LoggingEventArgs : System.EventArgs
    {
        #region Properties

        public Logger.Log4NetInfo LogInfo { get; private set; }

        /// <summary>
        /// If true the event is logged (default). If false it is not.
        /// </summary>
        public bool AllowLogging { get; set; }

        #endregion //end of Properties

        public delegate void EventHandler(Logger sender, LoggingEventArgs eventArgs);

        #region Constructors
        private LoggingEventArgs()
        {
        }

        public LoggingEventArgs(Logger.Log4NetInfo logInfo)

        {
            this.LogInfo = logInfo;
            this.AllowLogging = true;
        }


        #endregion //end of Constructors

        #region Public Methods

        /// <summary>
        /// Returns the rendered log message as an enumerable.
        /// </summary>
        /// <returns>
        /// </returns>
        public IEnumerable<string> RenderedLogMessages()
        {
            return this.LogInfo.LoggingEvents == null ? Enumerable.Empty<string>() : this.LogInfo.LoggingEvents.Select(m => m.RenderedMessage);
        }

        /// <summary>
        /// Returns the rendered log message
        /// </summary>
        /// <param name="idx"></param>
        /// <returns>
        /// </returns>
        public string RenderedLogMessage(int idx)
        {
            return this.LogInfo.LoggingEvents == null ? null : this.LogInfo.LoggingEvents[idx].RenderedMessage;
        }

        #endregion

        #region Invoke Event Static Methods

        public static bool InvokeEvent(Logger sender,
                                        Logger.Log4NetInfo logInfo,
                                        EventHandler invokeDelegate)
        {
            if (invokeDelegate != null)
            {
                var eventArgs = new LoggingEventArgs(logInfo);

                invokeDelegate(sender, eventArgs);
                return eventArgs.AllowLogging;
            }

            return true;
        }

        public static bool InvokeEvent(Logger sender,
                                        log4net.Core.LoggingEvent[] loggingEvents,
                                        EventHandler invokeDelegate)
        {
            if (invokeDelegate != null)
            {
                var eventArgs = new LoggingEventArgs(new Logger.Log4NetInfo() { LoggingEvents = loggingEvents });

                invokeDelegate(sender, eventArgs);
                return eventArgs.AllowLogging;
            }

            return true;
        }

        public static bool HasAssignedEvent(EventHandler invokeDelegate)
        {
            return invokeDelegate != null;
        }
        #endregion //end of Invoke Event Static Methods
    } //end of Event Argument Class LoggingEventArgs
}
