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
        public static readonly DateTime RunDateTime = DateTime.Now;
        public static string CommandLineArgsString = null;
        public static bool DebugMode = false;

        #region Console

        static public ConsoleDisplay ConsoleDeCompressFiles = null;
        static public ConsoleDisplay ConsoleTasksReadFiles = null;
        static public ConsoleDisplay ConsoleLogReadFiles = null;
        static public ConsoleDisplay ConsoleAnalyze = null;
        static public ConsoleDisplay ConsoleParsingDataTable = null;
        static public ConsoleDisplay ConsoleExcelWorkSheet = null;
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
            if(ConsoleDeCompressFiles != null)
            {
                if(sender is DSEDiagnosticFileParser.DiagnosticFile)
                {
                    var diagFile = (DSEDiagnosticFileParser.DiagnosticFile)sender;

                    if(diagFile.Catagory == DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile)
                    {
                        if(eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start))
                        {
                            ConsoleLogReadFiles.Increment(diagFile.ShortFilePath);
                        }
                        else if(eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End)
                                    || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel))
                        {
                            ConsoleLogReadFiles.TaskEnd(diagFile.ShortFilePath);
                        }
                    }
                    else if (diagFile.Catagory == DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.ZipFile)
                    {
                        if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start))
                        {
                            ConsoleDeCompressFiles.Increment(diagFile.ShortFilePath);
                        }
                        else if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End)
                                    || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel))
                        {
                            ConsoleDeCompressFiles.TaskEnd(diagFile.ShortFilePath);
                        }
                    }
                    else
                    {
                        if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start))
                        {
                            ConsoleTasksReadFiles.Increment(diagFile.ShortFilePath);
                        }
                        else if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End)
                                    || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel))
                        {
                            ConsoleTasksReadFiles.TaskEnd(diagFile.ShortFilePath);
                        }
                    }

                    if(eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel))
                    {
                        ConsoleWarnings.Increment(string.Format("Canceled processing for {0}", diagFile.ShortFilePath.FileName));
                    }

                    return;
                }

                if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Process)
                      || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Collection))
                {
                    if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start))
                    {
                        ConsoleTasksReadFiles.Increment(eventArgs.Message());
                    }
                    else if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel)
                                || eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End))
                    {
                        ConsoleTasksReadFiles.TaskEnd(eventArgs.Message());
                    }

                    if (eventArgs.Category.HasFlag(DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel))
                    {
                        ConsoleWarnings.Increment(eventArgs.Message());
                    }
                }
            }
        }

        private static void Instance_OnLoggingEvent(DSEDiagnosticLogger.Logger sender, DSEDiagnosticLogger.LoggingEventArgs eventArgs)
        {
            foreach (var item in eventArgs.LogInfo.LoggingEvents)
            {
                if(item.Level == log4net.Core.Level.Error || item.Level == log4net.Core.Level.Fatal)
                {                    
                    ConsoleErrors?.Increment(string.Format(@"Log: {0:yyyy-MM-dd\ HH\:mm\:ss.fff}", item.TimeStamp));
                }
                else if(item.Level == log4net.Core.Level.Warn)
                {
                    ConsoleWarnings?.Increment(string.Format(@"Log: {0:yyyy-MM-dd\ HH\:mm\:ss.fff}", item.TimeStamp));
                }

            }
        }

        private static string DetermineExcelTargetFile(DSEDiagnosticLibrary.Cluster cluster)
        {
           string processTF = string.Empty;

            if (DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange != null && !DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.IsEmpty())
            {
                processTF = string.Format("-{0:yyMMdd}To{1:MMdd}",
                                            DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Min.UtcDateTime,
                                            DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Max.UtcDateTime);
            }

            return string.Format(Properties.Settings.Default.ExcelFileNameGeneratedStringFormat,
                                    ParserSettings.DiagnosticPath,
                                    cluster == null ? "<clustername>" : (cluster.IsMaster ? "MasterCluster" : cluster.Name),
                                    RunDateTime,
                                    ParserSettings.Profile,
                                    processTF,
                                    ParserSettings.ExcelFileTemplatePath?.FileExtension ?? DSEDiagtnosticToExcel.LibrarySettings.ExcelFileExtension);
        }
        #endregion

        static int Main(string[] args)
        {
            #region Setup Exception handling, Argument Parsering

            Common.TimeZones.Convert(DateTime.Now, "UTC");

            Logger.Instance.InfoFormat("Starting {0} ({1}) Version: {2} RunAs: {3} RunTime Dir: {4}",
                                            Common.Functions.Instance.ApplicationName,
                                            Common.Functions.Instance.AssemblyFullName,
                                            Common.Functions.Instance.ApplicationVersion,
                                            Common.Functions.Instance.CurrentUserName,
                                            Common.Functions.Instance.ApplicationRunTimeDir);

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            System.Console.CancelKeyPress += Console_CancelKeyPress;

            CommandLineArgsString = string.Join(" ", args);

            Logger.Instance.InfoFormat("DSEDiagnosticConsoleApplication Main Start with Args: {0}", CommandLineArgsString);
            DSEDiagnosticFileParser.DiagnosticFile.OnException += DiagnosticFile_OnException;
            DSEDiagnosticFileParser.DiagnosticFile.OnProgression += DiagnosticFile_OnProgression;
            Logger.Instance.OnLoggingEvent += Instance_OnLoggingEvent;

            #region Arguments
            {
                var consoleArgs = new ConsoleArguments();

                try
                {
                    if (!consoleArgs.ParseSetArguments(args))
                    {
                        Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                        return 1;
                    }

                    if (ParserSettings.DiagnosticPath == null)
                    {
                        throw new ArgumentNullException("DiagnosticPath argument is required");
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

                    consoleArgs.ShowUsage();
                    Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                    return 1;
                }
                catch (System.Exception e)
                {
                    ConsoleDisplay.Console.WriteLine(e.Message);
                    ConsoleDisplay.Console.WriteLine("CommandLine: '{0}'", CommandLineArgsString);

                    consoleArgs.ShowUsage();
                    Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                    return 1;
                }

                if (ParserSettings.ExcelFilePath != null
                        && ParserSettings.ExcelFilePath.IsRelativePath
                        && ParserSettings.DiagnosticPath.IsAbsolutePath)
                {
                    IAbsolutePath absPath;

                    if (ParserSettings.ExcelFilePath.MakePathFrom((Common.IAbsolutePath)ParserSettings.DiagnosticPath, out absPath))
                    {
                        ParserSettings.ExcelFilePath = (Common.IFilePath)absPath;
                    }
                }

                if (ParserSettings.ExcelFilePath != null
                        && ParserSettings.ExcelFilePath.IsAbsolutePath
                        && ParserSettings.DiagnosticPath.IsRelativePath)
                {
                    IAbsolutePath absPath;

                    if (ParserSettings.ExcelFileTemplatePath.MakePathFrom((Common.IAbsolutePath)ParserSettings.ExcelFilePath, out absPath))
                    {
                        ParserSettings.ExcelFileTemplatePath = (Common.IFilePath)absPath;
                    }
                }

                Logger.Instance.InfoFormat("Settings:\r\n\t{0}", ParserSettings.SettingValues());
            }
            #endregion

            #region Console Display Setup

            ConsoleDisplay.Console.ClearScreen();
            ConsoleDisplay.Console.AdjustScreenStartBlock();

            ConsoleDisplay.Console.WriteLine(" ");
            ConsoleDisplay.Console.WriteLine("Diagnostic Source Folder: \"{0}\"", ParserSettings.DiagnosticPath);
            ConsoleDisplay.Console.WriteLine("Profile: \"{0}\"", ParserSettings.Profile);

            ConsoleDisplay.Console.WriteLine(" ");

            ConsoleTasksReadFiles = new ConsoleDisplay("File Tasks Completed: {0} Working: {1} Task: {2}");
            ConsoleDeCompressFiles = new ConsoleDisplay("Decompression Completed: {0} Working: {1} Task: {2}");
            ConsoleLogReadFiles = new ConsoleDisplay("Log Completed: {0}  Working: {1} Task: {2}");
            ConsoleAnalyze = new ConsoleDisplay("Analyze Processing: {0}  Working: {1} Task: {2}");
            ConsoleParsingDataTable = new ConsoleDisplay("DataTable Processing: {0}  Working: {1} Task: {2}");
            ConsoleExcelWorkSheet = new ConsoleDisplay("Excel WorkSheet: {0}  Working: {1} Task: {2}");
            ConsoleExcelWorkbook = new ConsoleDisplay("Excel WorkBook: {0}  Working: {1} Task: {2}");
            ConsoleWarnings = new ConsoleDisplay("Warnings: {0} Last: {2}", 2, false);
            ConsoleErrors = new ConsoleDisplay("Errors: {0} Last: {2}", 2, false);

            ConsoleDisplay.Console.ReserveRwWriteConsoleSpace(ConsoleDisplay.ConsoleRunningTimerTag, 2, -1);
            ConsoleDisplay.Console.ReserveRwWriteConsoleSpace("Prompt", 2, -1);
            ConsoleDisplay.Console.AdjustScreenToFitBasedOnStartBlock();

            ConsoleDisplay.Start();

            #endregion

            GCMonitor.GetInstance().StartGCMonitoring();

            #endregion

            var cancellationSource = new System.Threading.CancellationTokenSource();
            var diagParserTask = ProcessDSEDiagnosticFileParser(cancellationSource);
            var aggInfoStatsTask = ProcessAnalytics_LogInfo(diagParserTask, cancellationSource);
            var loadAllDataTableTask = LoadDataTables(diagParserTask, aggInfoStatsTask, cancellationSource);
            var loadExcelWorkBookTask = LoadExcelWorkbook(diagParserTask, loadAllDataTableTask, cancellationSource);

            #region Wait for Tasks to Complete
            {
                var tasks = new List<Task>() { diagParserTask, loadAllDataTableTask, loadExcelWorkBookTask };
                tasks.AddRange(aggInfoStatsTask);

                System.Threading.Tasks.Task.WaitAll(tasks.ToArray(), cancellationSource.Token);
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

        #region Canceled Processing
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
        #endregion
    }
}
