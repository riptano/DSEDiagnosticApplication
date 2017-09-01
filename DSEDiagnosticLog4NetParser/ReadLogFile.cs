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

        public async Task<ILogMessages> BeginProcessLogFile(IFilePath logFile)
        {
            this.LogFile = logFile;
            return await this.BeginProcessLogFile();
        }

        public ILogMessages ProcessLogFile(IFilePath logFile)
        {
            this.LogFile = logFile;
            var logMsgsTask = this.BeginProcessLogFile();

            logMsgsTask.Wait();

            if(logMsgsTask.IsCanceled)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                throw new OperationCanceledException("Log4Parser Canceled", this.CancellationToken);
            }
            else if(logMsgsTask.IsFaulted)
            {
                throw logMsgsTask.Exception;
            }

            return logMsgsTask.Result;
        }

        public async Task<ILogMessages> BeginProcessLogFile()
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

                    if (logMessage != null)
                    {
                        eventAction?.Invoke(this, logMessages, logMessage);
                    }
                }
            }

            this.Completed = true;
            return this.Log;
        }

        public ILogMessages ProcessLogFile()
        {
            var logMsgsTask = this.BeginProcessLogFile();

            logMsgsTask.Wait();

            if (logMsgsTask.IsCanceled)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                throw new OperationCanceledException("Log4Parser Canceled", this.CancellationToken);
            }
            else if (logMsgsTask.IsFaulted)
            {
                throw logMsgsTask.Exception;
            }

            return logMsgsTask.Result;
        }

        #endregion

        #region private members
        string _ianaTimeZone = null;
        #endregion

        #region Dispose Methods

        public bool Disposed { get; private set; }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
	        Dispose(true);
	        // This object will be cleaned up by the Dispose method.
	        // Therefore, you should call GC.SupressFinalize to
	        // take this object off the finalization queue
	        // and prevent finalization code for this object
	        // from executing a second time.
	        GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
	        // Check to see if Dispose has already been called.
	        if(!this.Disposed)
	        {

		        if(disposing)
		        {
                    // Dispose all managed resources.
                    this.Log?.Dispose();
                    this.Log = null;
                }

		        //Dispose of all unmanaged resources

		        // Note disposing has been done.
		        this.Disposed = true;

	        }
        }
        #endregion //end of Dispose Methods

    }
}
