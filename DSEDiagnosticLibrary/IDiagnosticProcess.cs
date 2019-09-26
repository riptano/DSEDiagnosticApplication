using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public interface IDiagnosticProcess
    {

        INode Node { get; }
        IFilePath File { get; }
        string DefaultClusterName { get; }
        string DefaultDataCenterName { get; }
        DateTimeRange ParseOnlyInTimeRange { get; }
        Version TargetDSEVersion { get; }
        int NbrItemsParsed { get; }
        DateTimeRange ParsedTimeRange { get;}

        /// <summary>
        /// True to indicate the process was sucessfully completed;
        /// </summary>
        bool Processed { get;}

        System.Exception Exception { get; }
        IEnumerable<string> ExceptionStrings { get;}

        int NbrItemGenerated { get; }

        int NbrWarnings { get; }
        int NbrErrors { get; }        
        CancellationToken CancellationToken { get; set; }
        bool Canceled { get; }
        Task DiagnosticTask { get; }

        TaskScheduler TaskScheduler { get;}

        IResult Result { get; }

        object Tag { get; set; }
    }
}
