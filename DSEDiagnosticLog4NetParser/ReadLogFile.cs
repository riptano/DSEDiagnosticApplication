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
        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern)
        {
            this.LogFile = logFilePath;
            this.Log4NetConversionPattern = log4netConversionPattern;
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, CancellationToken cancellationToken)
            : this(logFilePath, log4netConversionPattern)
        {           
            this.CancellationToken = cancellationToken;
        }

        public string Log4NetConversionPattern { get; set; }

        #region IReadLogFile
        public IFilePath LogFile { get; set; }
        public ILogMessages Log { get; private set; }
        public uint LinesRead { get; private set; }
        public int Errors { get { return this.Log == null ? 0 : this.Log.Errors.Count(); } }
        public CancellationToken CancellationToken { get; set; }

        public bool Completed { get; private set; }
        public async Task<ILogMessages> ProcessLogFile(IFilePath logFile)
        {
            this.LogFile = logFile;
            return await this.ProcessLogFile();
        }
        public async Task<ILogMessages> ProcessLogFile()
        {
            if (this.LogFile == null) throw new ArgumentNullException("LogFile");

            var logMessages = new LogMessages(this.LogFile, this.Log4NetConversionPattern);
            this.Log = logMessages;
            this.CancellationToken.ThrowIfCancellationRequested();

            using (var logStream = this.LogFile.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var readStream = new StreamReader(logStream))
            {
                LogMessage logMessage;
                string logLine;
                Task<string> nextLogLine = readStream.ReadLineAsync();

                for (;;)
                {
                    logLine = await nextLogLine;

                    if (logLine == null) break;

                    this.CancellationToken.ThrowIfCancellationRequested();

                    nextLogLine = readStream.ReadLineAsync();
                    logMessage = logMessages.AddMessage(logLine, ++this.LinesRead);                    
                }
            }

            this.Completed = true;
            return this.Log;
        }

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
