using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using Common.Patterns.Tasks;
using DSEDiagnosticParamsSettings;

namespace DSEDiagnosticConsoleApplication
{
    partial class Program
    {

        public static Task LoadDataBase(Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> diagParserTask,
                                        Task<System.Data.DataSet> loadAllDataTableTask,
                                        string diagnosticId,
                                        string connectionString,
                                        System.Threading.CancellationTokenSource cancellationSource)
        {
            var cluster = DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster();
            
            {
                if (cluster.IsMaster
                        && cluster.Nodes.HasAtLeastOneElement())
                {
                    System.Threading.Thread.Sleep(500);
                    cluster = DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster();

                    if (cluster.IsMaster)
                    {
                        if (diagParserTask.IsCompleted && loadAllDataTableTask.IsCompleted)
                        {
                            Logger.Instance.Error("File Parsing/DataTable tasks has completed but Cluster has not been properly defined! This usually indicates some type of issues with the diagnostic files.");
                            ConsoleErrors.Increment("File Parsing/DataTable Issues detected. Cluster not properly defined!");
                        }
                        else if (diagParserTask.IsFaulted || diagParserTask.IsCanceled || loadAllDataTableTask.IsFaulted || loadAllDataTableTask.IsCanceled)
                        {
                            Logger.Instance.Warn("File Parsing/DataTable tasks failed or was canceled! Cluster not properly defined.");
                            ConsoleWarnings.Increment("File Parsing/DataTable tasks failed or was canceled! Cluster not properly defined.");
                        }
                        else
                        {
                            Logger.Instance.Debug("Load Database is waiting on non-Master cluster");
                            ConsoleDatabase.Increment("Load Database is waiting on non-Master cluster");
                            Common.Patterns.Threading.LockFree.SpinWait(() =>
                            {
                                System.Threading.Thread.Sleep(500);
                                return !(cluster = DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster()).IsMaster;
                            });

                            ConsoleDatabase.TaskEnd("Load Database is waiting on non-Master cluster");
                            Logger.Instance.DebugFormat("Load Database is using Cluster \"{0}\"", cluster.Name);
                        }
                    }
                }

                if(string.IsNullOrEmpty(diagnosticId))
                    diagnosticId = string.Format(Properties.Settings.Default.DataBaseIdGeneratedStringFormat,
                                                    cluster == null ? "<clustername>" : (cluster.IsMaster ? "MasterCluster" : cluster.Name),
                                                    RunDateTime);
            }
                            
            var cancellationToken = cancellationSource.Token;

            var loadDataBaseTask = loadAllDataTableTask.ContinueWith(task =>
                                    {
                                        var dataSet = task.Result;
                                        var nodeToolRanges = cluster.Nodes
                                                                .Select(n => n.DSE.NodeToolDateRange?.Min)
                                                                .Where(i => i.HasValue)
                                                                .ToArray();
                                        var dataInfo = new DSEDiagnosticToDataSource.Data.RunInfo()
                                        {
                                            Aborted = AlreadyCanceled
                                                        || diagParserTask.IsCanceled
                                                        || loadAllDataTableTask.IsCanceled,
                                            AggregationDuration = ParserSettings.LogAggregationPeriod,
                                            AnalysisPeriodEnd = nodeToolRanges.DefaultIfEmpty()
                                                                        .Max(),
                                            AnalysisPeriodStart = nodeToolRanges.DefaultIfEmpty()
                                                                        .Min(),
                                            ApplicationArgs = CommandLineArgsString,
                                            ApplicationVersion = AppVersionString(),
                                            ClusterId = cluster.ClusterId,
                                            ClusterName = cluster.Name,
                                            DataQuality = DataQualityGrade(cluster),
                                            DiagnosticId = diagnosticId,
                                            Errors = (int)ConsoleErrors.Counter,
                                            Exceptions = (int)ConsoleExceptions.Counter,
                                            InsightClusterId = cluster.InsightClusterId,
                                            NbrDCs = cluster.DataCenters.Count(),
                                            NbrKeyspaces = cluster.Keyspaces.Count(),
                                            NbrNodes = cluster.Nodes.Count(),
                                            NbrTables = (int) cluster.Keyspaces.DefaultIfEmpty().Sum(k => k.Stats.Tables + k.Stats.MaterialViews),
                                            RunStartTime = RunDateTime.ConvertToOffSet(),
                                            Warnings = (int)ConsoleWarnings.Counter
                                        };

                                        DSEDiagnosticToDataSource.Data.BulkCopy(connectionString,
                                                                                dataSet.Tables.Cast<DataTable>(),
                                                                                dataInfo,
                                                                                cancellationSource,
                                                                                dt => ConsoleDatabase.Increment(dt.TableName),
                                                                                dt => ConsoleDatabase.TaskEnd(dt.TableName));               
                                    },
                                        cancellationToken,
                                        TaskContinuationOptions.OnlyOnRanToCompletion,
                                        TaskScheduler.Current);

            loadDataBaseTask.Then(() => ConsoleDatabase.Terminate());
            loadDataBaseTask.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                                TaskContinuationOptions.OnlyOnFaulted);
            loadDataBaseTask.ContinueWith(task => CanceledFaultProcessing(null),
                                                TaskContinuationOptions.OnlyOnCanceled);
            return loadDataBaseTask;
        }
    }
}
