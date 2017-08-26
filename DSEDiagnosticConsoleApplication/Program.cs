using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Patterns.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    class Program
    {
        public static readonly DateTime RunDateTime = DateTime.Now;
        public static string CommandLineArgsString = null;
        public static bool DebugMode = false;

        #region Console

        static public ConsoleDisplay ConsoleNonLogReadFiles = null;
        static public ConsoleDisplay ConsoleLogReadFiles = null;
        static public ConsoleDisplay ConsoleParsingNonLog = null;
        static public ConsoleDisplay ConsoleParsingLog = null;
        static public ConsoleDisplay ConsoleLogCount = null; //
        static public ConsoleDisplay ConsoleExcelNonLog = null;
        static public ConsoleDisplay ConsoleExcelLog = null;
        static public ConsoleDisplay ConsoleExcelLogStatus = null;
        static public ConsoleDisplay ConsoleExcel = null;
        static public ConsoleDisplay ConsoleExcelWorkbook = null;
        static public ConsoleDisplay ConsoleWarnings = null;
        static public ConsoleDisplay ConsoleErrors = null;

        #endregion

        #region Exception/Progression Handlers

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Logger.Instance.Warn("Application Aborted");
            Program.ConsoleErrors.Increment("Aborted");

            Logger.Flush();
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            ConsoleDisplay.End();

            GCMonitor.GetInstance().StopGCMonitoring();

            ConsoleDisplay.Console.SetReWriteToWriterPosition();
            ConsoleDisplay.Console.WriteLine();

            Logger.Instance.FatalFormat("Unhandled Exception Occurred! Exception Is \"{0}\" ({1}) Terminating Processing...",
                                            e.ExceptionObject?.GetType(),
                                            e.ExceptionObject is System.Exception ? ((System.Exception)e.ExceptionObject).Message : "<Not an Exception Object>");

            if (e.ExceptionObject is System.Exception)
            {
                Logger.Instance.Error("Unhandled Exception", ((System.Exception)e.ExceptionObject));
                ConsoleDisplay.Console.WriteLine("Unhandled Exception of \"{0}\" occurred", ((System.Exception)e.ExceptionObject).GetType().Name);
            }

            Logger.Instance.Info("DSEDiagnosticConsoleApplication Main Ended due to unhandled exception");
            Logger.Flush();

            ConsoleDisplay.Console.WriteLine();
            Common.ConsoleHelper.Prompt("Press Return to Exit (Unhandled Exception)", ConsoleColor.Gray, ConsoleColor.DarkRed);
            Environment.Exit(-1);
        }

        private static void DiagnosticFile_OnException(object sender, DSEDiagnosticFileParser.ExceptionEventArgs eventArgs)
        {
            Logger.Instance.Error("Exception within DSEDiagnosticFileParser", eventArgs.Exception);
            ConsoleErrors.Increment("Exception within DSEDiagnosticFileParser");
        }

        private static void DiagnosticFile_OnProgression(object sender, DSEDiagnosticFileParser.ProgressionEventArgs eventArgs)
        {
            if(ConsoleNonLogReadFiles != null
                    && (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Process)
                            || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Collection)))
            {
                if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start))
                {
                    ConsoleNonLogReadFiles.Increment(eventArgs.Message());
                }
                else if(eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel)
                            || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End))
                {
                    ConsoleNonLogReadFiles.TaskEnd(eventArgs.Message());
                }
            }
        }

        #endregion

        static int Main(string[] args)
        {
            #region Setup Exception handling, Argument Parsering

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            System.Console.CancelKeyPress += Console_CancelKeyPress;

            CommandLineArgsString = string.Join(" ", args);

            Logger.Instance.Info("DSEDiagnosticConsoleApplication Main Start");
            DSEDiagnosticFileParser.DiagnosticFile.OnException += DiagnosticFile_OnException;
            DSEDiagnosticFileParser.DiagnosticFile.OnProgression += DiagnosticFile_OnProgression;

            #region Arguments
            {
                var consoleArgs = new ConsoleArguments();
                var argParser = new CommandLineParser.CommandLineParser();

                try
                {
                    argParser.ExtractArgumentAttributes(consoleArgs);
                    argParser.ParseCommandLine(args);

                    if (!argParser.ParsingSucceeded)
                    {
                        argParser.ShowUsage();
                        return 1;
                    }


                    if (consoleArgs.Debug)
                    {
                        DebugMode = true;
                        Common.ConsoleHelper.Prompt("Attach Debugger and Press Return to Continue", ConsoleColor.Gray, ConsoleColor.DarkRed);
                        ConsoleDisplay.DisableAllConsoleWriter();
                    }
                }
                catch (CommandLineParser.Exceptions.CommandLineException e)
                {
                    ConsoleDisplay.Console.WriteLine(e.Message);
                    ConsoleDisplay.Console.WriteLine("CommandLine: '{0}'", CommandLineArgsString);

                    argParser.ShowUsage();
                    Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                    return 1;
                }
            }
            #endregion

            #region Console Display Setup

            ConsoleDisplay.Console.ClearScreen();
            ConsoleDisplay.Console.AdjustScreenStartBlock();

            ConsoleDisplay.Console.WriteLine(" ");
            ConsoleDisplay.Console.WriteLine("Diagnostic Folder Structure: \"{0}\"", ParserSettings.DiagFolderStruct);
            ConsoleDisplay.Console.WriteLine("Diagnostic Source Folder: \"{0}\"", ParserSettings.DiagnosticPath);
            //ConsoleDisplay.Console.WriteLine("Excel Target File: \"{0}\"", Common.Path.PathUtils.BuildDirectoryPath(argResult.Value.ExcelFilePath)?.PathResolved);

            ConsoleDisplay.Console.WriteLine(" ");

            ConsoleNonLogReadFiles = new ConsoleDisplay("Non-Log Files: {0} Working: {1} Task: {2}");
            ConsoleLogReadFiles = new ConsoleDisplay("Log Files: {0}  Working: {1} Task: {2}");
            ConsoleParsingNonLog = new ConsoleDisplay("Non-Log Processing: {0}  Working: {1} Task: {2}");
            ConsoleParsingLog = new ConsoleDisplay("Log Processing: {0}  Working: {1} Task: {2}");
            ConsoleLogCount = new ConsoleDisplay("Log Item Count: {0:###,###,##0}", 2, false); //
            ConsoleExcelNonLog = new ConsoleDisplay("Excel Non-Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLog = new ConsoleDisplay("Excel Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLogStatus = new ConsoleDisplay("Excel Status/Summary Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelWorkbook = new ConsoleDisplay("Excel Workbooks: {0} File: {2}");
            ConsoleWarnings = new ConsoleDisplay("Warnings: {0} Last: {2}", 2, false);
            ConsoleErrors = new ConsoleDisplay("Errors: {0} Last: {2}", 2, false);

            ConsoleDisplay.Console.ReserveRwWriteConsoleSpace(ConsoleDisplay.ConsoleRunningTimerTag, 2, -1);
            ConsoleDisplay.Console.ReserveRwWriteConsoleSpace("Prompt", 2, -1);
            ConsoleDisplay.Console.AdjustScreenToFitBasedOnStartBlock();

            ConsoleDisplay.Start();

            #endregion

            GCMonitor.GetInstance().StartGCMonitoring();

            #endregion

            #region DSEDiagnosticFileParser
            var cancellationSource = new System.Threading.CancellationTokenSource();
            var diagParserTask = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(ParserSettings.DiagnosticPath,
                                                                                     null,
                                                                                     null,
                                                                                     null,
                                                                                     cancellationSource);

            diagParserTask.Then(ignore => ConsoleNonLogReadFiles.Terminate());
            diagParserTask.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                            TaskContinuationOptions.OnlyOnFaulted);
            diagParserTask.ContinueWith(task => CanceledFaultProcessing(null),
                                            TaskContinuationOptions.OnlyOnCanceled);

            #endregion

            #region Load DataTables
            var datatableTasks = new List<Task<System.Data.DataTable>>();
            var loadAllDataTableTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<System.Data.DataSet>();
            {
                var cluster = DSEDiagnosticLibrary.Cluster.Clusters.FirstOrDefault(c => !c.IsMaster) ?? DSEDiagnosticLibrary.Cluster.Clusters.First();
                var loadDataTables = new DSEDiagnosticToDataTable.IDataTable[]
                {
                    new DSEDiagnosticToDataTable.ConfigDataTable(cluster, cancellationSource),
                    new DSEDiagnosticToDataTable.CQLDDLDataTable(cluster, cancellationSource, ParserSettings.IgnoreKeySpaces.ToArray()),
                    new DSEDiagnosticToDataTable.KeyspaceDataTable(cluster, cancellationSource, ParserSettings.IgnoreKeySpaces.ToArray()),
                    new DSEDiagnosticToDataTable.MachineDataTable(cluster, cancellationSource),
                    new DSEDiagnosticToDataTable.NodeDataTable(cluster, cancellationSource),
                    new DSEDiagnosticToDataTable.TokenRangesDataTable(cluster, cancellationSource)
                };

                loadDataTables.ForEach(ldtInstance =>
                {
                    ConsoleParsingNonLog.Increment(ldtInstance.Table.TableName);

                    var taskDataTable = diagParserTask.ContinueWith((task, instance) => ((DSEDiagnosticToDataTable.IDataTable)instance).LoadTable(),
                                                                                ldtInstance,
                                                                                cancellationSource.Token,
                                                                                TaskContinuationOptions.OnlyOnRanToCompletion,
                                                                                TaskScheduler.Default);
                    datatableTasks.Add(taskDataTable);
                    taskDataTable.Then(result => ConsoleParsingNonLog.TaskEnd(result.TableName));
                });

                loadAllDataTableTask = Task.Factory.ContinueWhenAll(datatableTasks.ToArray(),
                                                                    dtTasks =>
                                                                    {
                                                                        var dataSet = new System.Data.DataSet(string.Format("DSEDiagnostic Cluster {0}", cluster.Name));
                                                                        var dataTables = dtTasks.Select(t => t.Result).ToArray();
                                                                        //Work around where the default view is reset upon being added to the dataset....
                                                                        var dataViewProps = dataTables.Select(t => new Tuple<string,string,System.Data.DataViewRowState>(t.DefaultView.Sort, t.DefaultView.RowFilter, t.DefaultView.RowStateFilter)).ToArray();

                                                                        dataSet.Tables.AddRange(dataTables);

                                                                        for(int nIdx = 0; nIdx < dataTables.Count(); ++nIdx)
                                                                        {
                                                                            dataTables[nIdx].DefaultView.ApplyDefaultSort = false;
                                                                            dataTables[nIdx].DefaultView.AllowDelete = false;
                                                                            dataTables[nIdx].DefaultView.AllowEdit = false;
                                                                            dataTables[nIdx].DefaultView.AllowNew = false;
                                                                            dataTables[nIdx].DefaultView.Sort = dataViewProps[nIdx].Item1;
                                                                            dataTables[nIdx].DefaultView.RowFilter = dataViewProps[nIdx].Item2;
                                                                            dataTables[nIdx].DefaultView.RowStateFilter = dataViewProps[nIdx].Item3;
                                                                        }

                                                                        return dataSet;
                                                                    },
                                                                    cancellationSource.Token);
                loadAllDataTableTask.Then(result => ConsoleParsingNonLog.Terminate());
                loadAllDataTableTask.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                                            TaskContinuationOptions.OnlyOnFaulted);
                loadAllDataTableTask.ContinueWith(task => CanceledFaultProcessing(null),
                                                            TaskContinuationOptions.OnlyOnCanceled);

            }
            #endregion

            #region Load Excel
            {
                var loadExcel = new DSEDiagtnosticToExcel.LoadDataSet(loadAllDataTableTask,
                                                                            new Common.File.FilePathAbsolute(@"[DeskTop]\testa.xlsx"),
                                                                            cancellationSource);

                loadExcel.OnAction += (DSEDiagtnosticToExcel.IExcel sender, string action) =>
                                        {
                                            if(action == "Begin Loading")
                                            {
                                                if (sender.LoadTo == DSEDiagtnosticToExcel.LoadToTypes.WorkBook)
                                                {
                                                    ConsoleExcelLog.Increment(sender.WorkBookName);
                                                }
                                                else
                                                {
                                                    ConsoleExcelNonLog.Increment(sender.WorkSheetName);
                                                }
                                            }
                                            else if(action == "Loaded")
                                            {
                                                if (sender.LoadTo == DSEDiagtnosticToExcel.LoadToTypes.WorkBook)
                                                {
                                                    ConsoleExcelLog.TaskEnd(sender.WorkBookName);
                                                }
                                                else
                                                {
                                                    ConsoleExcelNonLog.TaskEnd(sender.WorkSheetName);
                                                }
                                            }
                                            else if(action == "Workbook Saved")
                                            {
                                                ConsoleExcelWorkbook.Increment(sender.ExcelTargetWorkbook);
                                            }
                                        };

                loadExcel.Load().Wait();
            }
            #endregion

            #region Termiate

            ConsoleDisplay.End();

            GCMonitor.GetInstance().StopGCMonitoring();

            Logger.Instance.Info("DSEDiagnosticConsoleApplication Main End");

            ConsoleDisplay.Console.SetReWriteToWriterPosition();
            ConsoleDisplay.Console.WriteLine();
            Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);

            #endregion

            return 0;
        }

        private static void LoadExcel_OnAction(DSEDiagtnosticToExcel.IExcel sender, string action)
        {
            throw new NotImplementedException();
        }

        static public volatile bool AlreadyCanceled = false;
        static bool _AlreadyCanceled = false;

        static void CanceledFaultProcessing(System.Exception ex)
        {
            if (!AlreadyCanceled && !Common.Patterns.Threading.LockFree.Exchange(ref _AlreadyCanceled, true))
            {
                AlreadyCanceled = true;
                ConsoleDisplay.End();

                GCMonitor.GetInstance().StopGCMonitoring();

                if(ex != null)
                {
                    Logger.Instance.Error("Fault Detected", ex);
                }

                Logger.Instance.Info("DSEDiagnosticConsoleApplication Main Ended from Fault or Canceled");

                ConsoleDisplay.Console.SetReWriteToWriterPosition();
                ConsoleDisplay.Console.WriteLine();
                Common.ConsoleHelper.Prompt(@"Press Return to Exit (Fault/Canceled)", ConsoleColor.Gray, ConsoleColor.DarkRed);
                Environment.Exit(-1);
            }
        }
    }
}
