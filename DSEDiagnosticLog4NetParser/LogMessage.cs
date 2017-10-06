using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticLog4NetParser
{
    public sealed class LogMessage : ILogMessage
    {
        public LogMessage(uint logLinePos, int originalLineHashCode)
        {
            this.LogLinePosition = logLinePos;
            this.OriginalLineHashCode = originalLineHashCode;
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

        public int OriginalLineHashCode { get; }

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

        #region overrides

        public override string ToString()
        {
            return this.ToString(LibrarySettings.LogMessageToStringMaxLength);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="maxMessageLength">if a negitive number, complete message is used</param>
        /// <returns></returns>
        public string ToString(int maxMessageLength)
        {
            return string.Format("{0}\t{1:d\\.hh\\:mm\\:ss\\.fff}\t{2:yyyy-MM-dd hh\\:mm\\:ss\\.fff K}\t{3}\t{4}\t{5}.{6}\t{7}{8}",
                                    this.LogLinePosition,
                                    this.LogTimeSpan,
                                    this.LogDateTimewTZOffset,
                                    this.Level,
                                    this.ThreadId,
                                    this.FileName,
                                    this.FileLine,
                                    maxMessageLength >= 0 && this.Message.Length > maxMessageLength ? this.Message.Substring(0, maxMessageLength) + "..." : this.Message,
                                    this._extraMessages.Count > 0 ? string.Format("\t<{0} Additional Messages>", this._extraMessages.Count) : string.Empty);
        }

        #endregion
    }

}
