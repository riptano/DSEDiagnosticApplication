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
        static Task<IEnumerable<DSEDiagnosticLibrary.IAggregatedStats>>[] ProcessAnalytics_LogInfo(Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> diagParserTask, System.Threading.CancellationTokenSource cancellationSource)
        {
            Task<IEnumerable<DSEDiagnosticLibrary.IAggregatedStats>>[] tasks = new Task<IEnumerable<DSEDiagnosticLibrary.IAggregatedStats>>[2];

            tasks[0] = diagParserTask.ContinueWith((task, ignore) =>
            {
                ConsoleAnalyze.Increment("LogFileStats");
                var cluster = DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster();
                var analyticInstance = new DSEDiagnosticAnalytics.LogFileStats(cluster, cancellationSource.Token);

                var logStats = analyticInstance.ComputeStats();
                var diagLogTasks = diagParserTask.Result
                                            .Where(tsk => tsk is DSEDiagnosticFileParser.file_cassandra_log4net)
                                            .Cast<DSEDiagnosticFileParser.file_cassandra_log4net>()
                                            .Select(tsk => tsk.Result)
                                            .Cast<DSEDiagnosticFileParser.file_cassandra_log4net.LogResults>()
                                            .Select(result => result.ResultsTask)
                                            .Where(result => result != null)
                                            .ToArray();

                System.Threading.Tasks.Task.WaitAll(diagLogTasks);

                return logStats;
            },
                                        null,
                                        cancellationSource.Token,
                                        TaskContinuationOptions.OnlyOnRanToCompletion,
                                        TaskScheduler.Default);
            tasks[0].Then(result => ConsoleAnalyze.TaskEnd("LogFileStats"));

            tasks[1] = diagParserTask.ContinueWith((task, ignore) =>
            {
                ConsoleAnalyze.Increment("Common PK Stats");
                var cluster = DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster();
                var analyticInstance = new DSEDiagnosticAnalytics.DataModelDCPK(cluster, cancellationSource.Token, ParserSettings.IgnoreKeySpaces.ToArray());

                return analyticInstance.ComputeStats();
            },
                                        null,
                                        cancellationSource.Token,
                                        TaskContinuationOptions.OnlyOnRanToCompletion,
                                        TaskScheduler.Default);
            tasks[1].Then(result => ConsoleAnalyze.TaskEnd("Common PK Stats"));

            var analysisTasks = Task.Factory.ContinueWhenAll(tasks,
                                                                t => t.Cast<Task<IEnumerable<DSEDiagnosticLibrary.IAggregatedStats>>>().SelectMany(r => r.Result));

            analysisTasks.Then(result => ConsoleAnalyze.Terminate());
            analysisTasks.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                                        TaskContinuationOptions.OnlyOnFaulted);
            analysisTasks.ContinueWith(task => CanceledFaultProcessing(null),
                                                        TaskContinuationOptions.OnlyOnCanceled);

            return tasks;
        }
    }
}
