using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Patterns.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    partial class Program
    {
        static Task<IEnumerable<DSEDiagnosticLibrary.IAggregatedStats>> ProcessAnalytics_LogInfo(Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> diagParserTask, System.Threading.CancellationTokenSource cancellationSource)
        {
            var logInfoStatsTask = diagParserTask.ContinueWith((task, ignore) =>
            {
                ConsoleAnalyze.Increment("LogFileStats");
                var cluster = DSEDiagnosticLibrary.Cluster.Clusters.FirstOrDefault(c => !c.IsMaster) ?? DSEDiagnosticLibrary.Cluster.Clusters.First();
                var analyticInstance = new DSEDiagnosticAnalytics.LogFileStats(cluster, cancellationSource.Token);

                return analyticInstance.ComputeStats();
            },
                                        null,
                                        cancellationSource.Token,
                                        TaskContinuationOptions.OnlyOnRanToCompletion,
                                        TaskScheduler.Default);
            logInfoStatsTask.Then(result => ConsoleAnalyze.TaskEnd("LogFileStats"));

            logInfoStatsTask.Then(result => ConsoleAnalyze.Terminate());
            logInfoStatsTask.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                                        TaskContinuationOptions.OnlyOnFaulted);
            logInfoStatsTask.ContinueWith(task => CanceledFaultProcessing(null),
                                                        TaskContinuationOptions.OnlyOnCanceled);
            return logInfoStatsTask;
        }
    }
}
