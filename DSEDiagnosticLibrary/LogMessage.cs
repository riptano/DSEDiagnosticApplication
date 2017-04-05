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
        TimeSpan LogTimeSpan { get; }
        string ThreadId { get; }
        string FileName { get;  }
        int FileLine { get; }
        string Message { get; }
        IEnumerable<string> ExtraMessages { get; }

        uint LogLinePosition { get; }
    }

    public interface ILogMessages : IDisposable
    {
        IFilePath LogFile { get; }
        IEnumerable<ILogMessage> Messages { get; }
        DateTimeRange LogTimeRange { get; }
        IEnumerable<string> Errors { get; }

    }

    /// <summary>
    /// Disposing will release all parsed messages.
    /// </summary>
    public interface IReadLogFile : IDisposable
    {
        IFilePath LogFile { get; set; }
        ILogMessages Log { get; }
        uint LinesRead { get; }
        int Errors { get; }
        CancellationToken CancellationToken { get; set; }        
        bool Completed { get; }

        Task<ILogMessages> ProcessLogFile(IFilePath logFile);
        Task<ILogMessages> ProcessLogFile();
    }
}
