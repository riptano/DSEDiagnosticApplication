using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticLog4NetParser
{
    public sealed class LogMessage : ILogMessage
    {
        public LogMessage(uint logLinePos)
        {
            this.LogLinePosition = logLinePos;
        }

        public LogLevels Level { get; set; }
        public DateTime LogDateTime { get; set; }
        /// <summary>
        /// LogDateTime&apos;s value based on the Node&apos;s timezone.
        /// If a node instance is not provided this value will be set to LogDateTime using UTC TZ.
        /// </summary>
        public DateTimeOffset LogDateTimewTZOffset { get; set; }
        /// <summary>
        /// Populated by the log4net &apos;timestamp&apos; pattern name.        
        /// </summary>
        /// <remarks>
        /// Log4net timestamp is the number of milliseconds elapsed since the start of the application until the creation of the logging event.
        /// </remarks>
        public TimeSpan LogTimeSpan { get; set; }
        public string ThreadId { get; set; }
        public string FileName { get; set; }
        public int FileLine { get; set; }
        public string Message { get; set; }

        private List<string> _extraMessages = new List<string>();
        public IEnumerable<string> ExtraMessages { get { return this._extraMessages; } }

        public uint LogLinePosition { get; set; }

        public void AddExtraMessage(string extraMessage)
        {
            extraMessage = extraMessage?.Trim();

            if (!string.IsNullOrEmpty(extraMessage))
            {
                this._extraMessages.Add(extraMessage);
            }
        }
    }

}
