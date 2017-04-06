using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticLog4NetParser
{
    public sealed class ReadLogFile : IReadLogFile
    {
        public ReadLogFile() { }
        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, INode node = null)
        {
            this.LogFile = logFilePath;
            this.Log4NetConversionPattern = log4netConversionPattern;
            this.Node = node;
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, CancellationToken cancellationToken, INode node = null)
            : this(logFilePath, log4netConversionPattern, node)
        {
            this.CancellationToken = cancellationToken;
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, string ianaTimezone)
            : this(logFilePath, log4netConversionPattern)
        {
            this._ianaTimeZone = ianaTimezone;
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, CancellationToken cancellationToken, string ianaTimezone)
            : this(logFilePath, log4netConversionPattern, cancellationToken)
        {
            this._ianaTimeZone = ianaTimezone;
        }

        public string Log4NetConversionPattern { get; set; }

        #region IReadLogFile
        public INode Node { get; private set; }
        public IFilePath LogFile { get; set; }
        public ILogMessages Log { get; private set; }
        public uint LinesRead { get; private set; }
        public int Errors { get { return this.Log == null ? 0 : this.Log.Errors.Count(); } }
        public CancellationToken CancellationToken { get; set; }

        public bool Completed { get; private set; }
        public event Action<IReadLogFile, ILogMessages, ILogMessage> ProcessedLogLineAction;

        public async Task<ILogMessages> ProcessLogFile(IFilePath logFile)
        {
            this.LogFile = logFile;
            return await this.ProcessLogFile();
        }
        public async Task<ILogMessages> ProcessLogFile()
        {
            if (this.LogFile == null) throw new ArgumentNullException("LogFile");

            var logMessages = string.IsNullOrEmpty(this._ianaTimeZone) || this.Node != null
                                ? new LogMessages(this.LogFile, this.Log4NetConversionPattern, this.Node)
                                : new LogMessages(this.LogFile, this.Log4NetConversionPattern, this._ianaTimeZone);
            this.Log = logMessages;
            this.CancellationToken.ThrowIfCancellationRequested();

            using (var logStream = this.LogFile.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var readStream = new StreamReader(logStream))
            {
                LogMessage logMessage;
                string logLine;
                Task<string> nextLogLine = readStream.ReadLineAsync();
                var eventAction = this.ProcessedLogLineAction;

                for (;;)
                {
                    logLine = await nextLogLine;

                    if (logLine == null) break;

                    this.CancellationToken.ThrowIfCancellationRequested();

                    nextLogLine = readStream.ReadLineAsync();
                    logMessage = logMessages.AddMessage(logLine, ++this.LinesRead);

                    eventAction?.Invoke(this, logMessages, logMessage);
                }
            }

            this.Completed = true;
            return this.Log;
        }

        #endregion

        #region private members
        string _ianaTimeZone = null;
        #endregion

        #region IDisposable
        public void Dispose()
        {
            if (this.Log != null) this.Log.Dispose();
            this.Log = null;
        }

        #endregion

    }
}
