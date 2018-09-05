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
        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, INode node = null, DateTimeOffsetRange logTimeFrame = null)
        {
            this.LogFile = logFilePath;
            this.Log4NetConversionPattern = log4netConversionPattern;
            this.Node = node;

            this.Node?.UpdateDSENodeToolDateRange();

            if (logTimeFrame == null)
            {
                if (this.Node?.Machine.TimeZone == null)
                {
                    if(string.IsNullOrEmpty(this.Node?.Machine.TimeZoneName))
                    {
                        DSEDiagnosticLogger.Logger.Instance.WarnFormat("Timezone instance for Node {0} with File \"{1}\" is missing!",
                                                                        this.Node,
                                                                        logFilePath);    
                    }
                    else
                    {
                        DSEDiagnosticLogger.Logger.Instance.WarnFormat("Timezone instance for Node {0} with File \"{1}\" is missing but a time zone name of \"{2}\" found! Time zone is still ignored.",
                                                                        this.Node,
                                                                        logFilePath,
                                                                        this.Node.Machine.TimeZoneName);
                    }
                }
                else if(this.Node.Machine.UsesDefaultTZ && DSEDiagnosticLogger.Logger.Instance.IsDebugEnabled)
                {
                    DSEDiagnosticLogger.Logger.Instance.DebugFormat("Using Default Timezone instance ({2}) for Node {0} with File \"{1}\".",
                                                                    this.Node,
                                                                    logFilePath,
                                                                    this.Node.Machine.TimeZone?.Name);
                }
            }
            else
            {
                if (this.Node == null || this.Node.Machine.TimeZone == null)
                {
                    DSEDiagnosticLogger.Logger.Instance.WarnFormat("Using Log Time Range of {0} for Node {1} with File \"{2}\" because Node's TimeZone information is missing!", logTimeFrame, this.Node, logFilePath);
                    this.LogTimeFrame = new DateTimeOffsetRange(logTimeFrame);
                }
                else
                {
                    this.LogTimeFrame = new DateTimeOffsetRange(logTimeFrame.Min == DateTimeOffset.MinValue
                                                                    ? DateTimeOffset.MinValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Min, this.Node.Machine.TimeZone),
                                                                  logTimeFrame.Max == DateTimeOffset.MaxValue
                                                                    ? DateTimeOffset.MaxValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Max, this.Node.Machine.TimeZone));
                }
            }
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, CancellationToken cancellationToken, INode node = null, DateTimeOffsetRange logTimeFrame = null)
            : this(logFilePath, log4netConversionPattern, node, logTimeFrame)
        {
            this.CancellationToken = cancellationToken;
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, string ianaTimezone, DateTimeOffsetRange logTimeFrame = null)
            : this(logFilePath, log4netConversionPattern)
        {
            this._ianaTimeZone = ianaTimezone;

            if (logTimeFrame != null)
            {
                if (string.IsNullOrEmpty(this._ianaTimeZone))
                {
                    DSEDiagnosticLogger.Logger.Instance.WarnFormat("Disabled Log Time Range of {0} with File \"{1}\" because TimeZone information is missing", logTimeFrame, logFilePath);
                }
                else
                {
                    this.LogTimeFrame = new DateTimeOffsetRange(logTimeFrame.Min == DateTimeOffset.MinValue
                                                                    ? DateTimeOffset.MinValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Min, this._ianaTimeZone, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName).DateTime,
                                                                logTimeFrame.Max == DateTimeOffset.MaxValue
                                                                    ? DateTimeOffset.MaxValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Max, this._ianaTimeZone, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName).DateTime);
                }
            }
        }

        public ReadLogFile(IFilePath logFilePath, string log4netConversionPattern, CancellationToken cancellationToken, string ianaTimezone, DateTimeOffsetRange logTimeFrame = null)
            : this(logFilePath, log4netConversionPattern, cancellationToken)
        {
            this._ianaTimeZone = ianaTimezone;

            if (logTimeFrame != null)
            {
                if (string.IsNullOrEmpty(this._ianaTimeZone))
                {
                    DSEDiagnosticLogger.Logger.Instance.WarnFormat("Disabled Log Time Range of {0} with File \"{1}\" because TimeZone information is missing", logTimeFrame, logFilePath);
                }
                else
                {
                    this.LogTimeFrame = new DateTimeOffsetRange(logTimeFrame.Min == DateTimeOffset.MinValue
                                                                    ? DateTimeOffset.MinValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Min, this._ianaTimeZone, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName).DateTime,
                                                                logTimeFrame.Max == DateTimeOffset.MaxValue
                                                                    ? DateTimeOffset.MaxValue
                                                                    : Common.TimeZones.Convert(logTimeFrame.Max, this._ianaTimeZone, Common.Patterns.TimeZoneInfo.ZoneNameTypes.IANATZName).DateTime);
                }
            }
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

        /// <summary>
        /// If defined the time frame log entries will be parsed. The Time Frame will be in the Node&apos;s timezone.
        /// </summary>
        public DateTimeOffsetRange LogTimeFrame { get; }

        public event Action<IReadLogFile, ILogMessages, ILogMessage> ProcessedLogLineAction;

        public async Task<ILogMessages> BeginProcessLogFile(IFilePath logFile)
        {
            this.LogFile = logFile;
            return await this.BeginProcessLogFile();
        }

        public ILogMessages ProcessLogFile(IFilePath logFile)
        {
            return this.BeginProcessLogFile(logFile).Result;           
        }

        public async Task<ILogMessages> BeginProcessLogFile()
        {
            var task = Task.Run(() => this.ProcessLogFile());
            return await task;           
        }

        public ILogMessages ProcessLogFile()
        {
            bool skippedLines = false;

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
                LogMessage lastLogMessage = null;
                string logLine;
                var eventAction = this.ProcessedLogLineAction;
                short logRangeCheck;

                logMessages.CompletionStatus = LogCompletionStatus.InProcess;

                for (;;)
                {
                    logLine = readStream.ReadLine();

                    if (logLine == null) break;

                    this.CancellationToken.ThrowIfCancellationRequested();
                    
                    logMessage = logMessages.AddMessage(logLine, ++this.LinesRead);

                    if (logMessage == null)
                    {
                        if (lastLogMessage != null
                                && lastLogMessage.ExtraMessages.HasAtLeastOneElement()
                                && ReferenceEquals(logMessages.Messages.LastOrDefault(), lastLogMessage))
                        {
                            eventAction?.Invoke(this, logMessages, lastLogMessage);
                        }
                        lastLogMessage = logMessage;
                    }
                    else
                    {
                        if(ReferenceEquals(logMessage, lastLogMessage))
                        {
                            continue;
                        }
                        else if(lastLogMessage != null
                                    && lastLogMessage.ExtraMessages.HasAtLeastOneElement()
                                    && ReferenceEquals(logMessages.Messages.TakeLast(2).First(), lastLogMessage))
                        {
                            eventAction?.Invoke(this, logMessages, lastLogMessage);
                        }

                        lastLogMessage = logMessage;
                        logRangeCheck = this.LogFileWithinTimeRange(logMessage, logMessages, readStream);

                        if(logRangeCheck > 0)
                        {
                            eventAction?.Invoke(this, logMessages, logMessage);
                        }
                        else if (logRangeCheck == 0)
                        {
                            logMessages.RemoveMessage();
                            skippedLines = true;
                        }
                        else if (logRangeCheck == -1)
                        {
                            if(this.LinesRead > 1)
                            {
                                logMessages.CompletionStatus = LogCompletionStatus.PartiallyCompleted;
                            }
                            else
                            {
                                logMessages.CompletionStatus = LogCompletionStatus.Skipped;
                            }

                            logMessages.RemoveMessage();
                            break;
                        }
                        else if (logRangeCheck == -2)
                        {
                            logMessages.CompletionStatus = LogCompletionStatus.Skipped;
                            logMessages.RemoveMessage();
                            break;
                        }
                    }
                }

                if (lastLogMessage != null
                        && lastLogMessage.ExtraMessages.HasAtLeastOneElement()
                        && ReferenceEquals(logMessages.Messages.LastOrDefault(), lastLogMessage))
                {
                    eventAction?.Invoke(this, logMessages, lastLogMessage);
                }
            }

            if (logMessages.CompletionStatus == LogCompletionStatus.InProcess)
            {
                logMessages.CompletionStatus = skippedLines ? LogCompletionStatus.PartiallyCompleted : LogCompletionStatus.Completed;
            }

            this.Completed = true;
            return this.Log;
        }
        
        public ILogMessages ReadLogFileTimeRange(IFilePath logFile)
        {
            this.LogFile = logFile;
            return this.ReadLogFileTimeRange();
        }

        public ILogMessages ReadLogFileTimeRange()
        {
            if (this.LogFile == null) throw new ArgumentNullException("LogFile");

            var logMessages = string.IsNullOrEmpty(this._ianaTimeZone) || this.Node != null
                                ? new LogMessages(this.LogFile, this.Log4NetConversionPattern, this.Node)
                                : new LogMessages(this.LogFile, this.Log4NetConversionPattern, this._ianaTimeZone);
            this.Log = logMessages;
            this.CancellationToken.ThrowIfCancellationRequested();
            logMessages.CompletionStatus = LogCompletionStatus.InProcess;

            using (var logStream = this.LogFile.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var readStream = new StreamReader(logStream))
            {
                LogMessage logMessage = null;
                string logLine = readStream.ReadLine();

                while(logLine != null && (logMessage = logMessages.AddMessage(logLine, 0)) == null)
                {
                    logLine = readStream.ReadLine();
                }

                if (logMessage == null)
                {
                    logMessages.CompletionStatus = LogCompletionStatus.NotProcessed;
                }
                else
                {
                    logMessages.RemoveMessage();
                    this.LastLogLineWithinTimeRange(logMessage, logMessages, readStream, true);
                    logMessages.CompletionStatus = LogCompletionStatus.ReadLogTimeRange;
                }
            }

            if(this.Log.LogTimeRange != null && this.LogTimeFrame != null)
            {
                if(!this.LogTimeFrame.IsBetween(this.Log.LogTimeRange))
                {
                    var interRange = this.LogTimeFrame.Intersect(this.Log.LogTimeRange);

                    if (interRange == null)
                    {
                        logMessages.CompletionStatus = LogCompletionStatus.Skipped;
                    }
                    else
                    {
                        ((LogMessages)this.Log).SetStartingTime(interRange.Min);
                        ((LogMessages)this.Log).SetEndingTime(interRange.Max);
                    }
                }
            }

            this.Completed = true;
            return this.Log;
        }

        #endregion

        #region private members
        string _ianaTimeZone = null;

        /// <summary>
        ///
        /// </summary>
        /// <param name="logMessage"></param>
        /// <returns>
        /// 1 -- Process Msg
        /// 0 -- Don't Process Msg
        /// -1 -- Don't process remaining file
        /// -2 -- Skip File
        /// </returns>
        private short LogFileWithinTimeRange(LogMessage logMessage, LogMessages logMessages, StreamReader streamReader)
        {
            if (this.LogTimeFrame == null || this.LogTimeFrame.IsBetween(logMessage.LogDateTimewTZOffset))
            {
                return 1;
            }

            if (logMessage.LogDateTimewTZOffset > this.LogTimeFrame.Max)
            {
                DSEDiagnosticLogger.Logger.Instance.DebugFormat("Reminding Log File skipped because Log Entries ({0}) exceed Log Time Range of {1} for File \"{2}\"",
                                                                    logMessage.LogDateTimewTZOffset,
                                                                    this.LogTimeFrame,
                                                                    this.LogFile);

                this.LastLogLineWithinTimeRange(logMessage, logMessages, streamReader, true);
                return -1;
            }

            if (logMessage.LogLinePosition > 1)
            {
                return 0;
            }

            return LastLogLineWithinTimeRange(logMessage, logMessages, streamReader);
        }

        private short LastLogLineWithinTimeRange(LogMessage logMessage, LogMessages logMessages, StreamReader streamReader, bool justReadLastEntries = false, long readBackLength = 1024)
        {
            bool wentBackward = false;
            DateTimeOffset lastDateTime = DateTimeOffset.MaxValue;
            string nextLine = null;
            LogMessage testLogMsg = null;
            string readLine;

            this.CancellationToken.ThrowIfCancellationRequested();

            if (streamReader.BaseStream.Length > readBackLength)
            {
                nextLine = streamReader.ReadLine();

                streamReader.DiscardBufferedData();
                streamReader.BaseStream.Seek(readBackLength * -1, System.IO.SeekOrigin.End);
                wentBackward = true;
            }

            using (var tmpLogMsgs = new LogMessages(logMessages))
            {
                while ((readLine = streamReader.ReadLine()) != null)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    testLogMsg = tmpLogMsgs.AddMessage(readLine, 0, true);
                    if (testLogMsg != null)
                    {
                        lastDateTime = testLogMsg.LogDateTimewTZOffset;
                    }
                }
            }

            //Need to go further back...
            if(lastDateTime == DateTimeOffset.MaxValue && wentBackward)
            {
                return this.LastLogLineWithinTimeRange(logMessage, logMessages, streamReader, justReadLastEntries, readBackLength * 2);
            }

            //Some Log entry within the file is within time frame (current log entry is not)
            if (!justReadLastEntries && lastDateTime >= this.LogTimeFrame.Min)
            {
                streamReader.DiscardBufferedData();
                streamReader.BaseStream.Seek(0, System.IO.SeekOrigin.Begin);

                while ((readLine = streamReader.ReadLine()) != null)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    if (nextLine == null || nextLine == readLine)
                    {
                        break;
                    }
                }

                DSEDiagnosticLogger.Logger.Instance.DebugFormat("Forwarding Log File where first Log Entries ({0}) is before log Time Range of {1} but last entry ({3}) is within range for File \"{2}\"",
                                                                logMessage.LogDateTimewTZOffset,
                                                                this.LogTimeFrame,
                                                                this.LogFile,
                                                                lastDateTime);
                return 0;
            }

            logMessages.SetEndingTimeRange(lastDateTime);

            if (!justReadLastEntries)
            {
                DSEDiagnosticLogger.Logger.Instance.DebugFormat("Log File \"{0}\" will be skipped since it does not meet log range of {1}. Log Entries Range is from {2} to {3}",
                                                                   this.LogFile,
                                                                   this.LogTimeFrame,
                                                                   logMessage.LogDateTimewTZOffset,
                                                                   lastDateTime);
            }

            return -2;
        }


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
