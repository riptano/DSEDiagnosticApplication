using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public enum LogLevels
    {
        Unknown = 0,
        Debug,
        Info,
        Warn,
        Error,
        Fatal
    }

    public interface ILogMessage
    {
       LogLevels Level { get;}
        DateTime LogDateTime { get; }
        /// <summary>
        /// LogDateTime&apos;s value based on the Node&apos;s timezone.
        /// If a node instance is not provided this value will be set to LogDateTime using UTC TZ.
        /// </summary>
        DateTimeOffset LogDateTimewTZOffset { get; }
        /// <summary>
        /// Populated by the log4net &apos;timestamp&apos; pattern name.
        /// </summary>
        /// <remarks>
        /// Log4net timestamp is the number of milliseconds elapsed since the start of the application until the creation of the logging event.
        /// </remarks>
        TimeSpan LogTimeSpan { get; }
        string ThreadId { get; }
        string FileName { get;  }
        int FileLine { get; }
        string Message { get; }
        IEnumerable<string> ExtraMessages { get; }

        uint LogLinePosition { get; }

        int OriginalLineHashCode { get; }

        bool TraceEnabled { get; }
    }

    public enum LogCompletionStatus
    {
        NotProcessed = 0,
        InProcess,
        Skipped,
        PartiallyCompleted,
        Completed,
        ReadLogTimeRange
    }

    public interface ILogMessages : IDisposable
    {
        INode Node { get; }
        string IANATimeZoneName { get; }
        IFilePath LogFile { get; }
        IEnumerable<ILogMessage> Messages { get; }
        DateTimeOffsetRange LogTimeRange { get; }
        DateTimeOffsetRange LogFileTimeRange { get; }

        IEnumerable<string> Errors { get; }

        LogCompletionStatus CompletionStatus { get; }

        bool Disposed { get; }
    }

    /// <summary>
    /// Disposing will release all parsed messages.
    /// </summary>
    public interface IReadLogFile : IDisposable
    {
        INode Node { get; }
        IFilePath LogFile { get; set; }
        ILogMessages Log { get; }
        uint LinesRead { get; }
        int Errors { get; }
        CancellationToken CancellationToken { get; set; }
        bool Completed { get; }
        bool IgnoreErrors { get; set; }
        /// <summary>
        /// If defined the time frame log entries will be parsed. The Time Frame will be in the Node&apos;s timezone.
        /// </summary>
        DateTimeOffsetRange LogTimeFrame { get; }

        /// <summary>
        /// Invoked after a Log Line is processed.
        /// </summary>
        /// <remarks>
        /// It is up to the consumer to properly handle exceptions including cancellations. This event is invoked synchronously.
        /// </remarks>
        event Action<IReadLogFile, ILogMessages, ILogMessage> ProcessedLogLineAction;

        Task<ILogMessages> BeginProcessLogFile(IFilePath logFile);
        ILogMessages ProcessLogFile(IFilePath logFile);        
        Task<ILogMessages> BeginProcessLogFile();
        ILogMessages ProcessLogFile();

        ILogMessages ReadLogFileTimeRange();
        ILogMessages ReadLogFileTimeRange(IFilePath logFile);

        bool Disposed { get; }
    }
}
