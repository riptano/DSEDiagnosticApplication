using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

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

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            ConsoleDisplay.Console.WriteLine(" ");

            Logger.Instance.FatalFormat("Unhandled Exception Occurred! Exception Is \"{0}\" ({1}) Terminating Processing: {2}",
                                            e.ExceptionObject.GetType(),
                                            e.ExceptionObject is System.Exception ? ((System.Exception)e.ExceptionObject).Message : "<Not an Exception Object>",
                                            e.IsTerminating);

            if (e.ExceptionObject is System.Exception)
            {
                Logger.Instance.Error("Unhandled Exception", ((System.Exception)e.ExceptionObject));
                Program.ConsoleErrors.Increment("Unhandled Exception");
            }

            //ExceptionOccurred = true;
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

        static void Main(string[] args)
        {
            #region Setup Exception handling, Argument Parsering

            CommandLineArgsString = string.Join(" ", args);

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

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
                        return;
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
                    return;
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
            ConsoleExcel = new ConsoleDisplay("Excel: {0}  Working: {1} WorkSheet: {2}");
            ConsoleExcelWorkbook = new ConsoleDisplay("Excel Workbooks: {0} File: {2}");
            ConsoleWarnings = new ConsoleDisplay("Warnings: {0} Last: {2}", 2, false);
            ConsoleErrors = new ConsoleDisplay("Errors: {0} Last: {2}", 2, false);

            ConsoleDisplay.Console.ReserveRwWriteConsoleSpace("Prompt", 2, -1);
            ConsoleDisplay.Console.AdjustScreenToFitBasedOnStartBlock();

            ConsoleDisplay.Start();

            #endregion

            GCMonitor.GetInstance().StartGCMonitoring();

            #endregion

            #region DSEDiagnosticFileParser
            var diagParserTask = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(ParserSettings.DiagnosticPath);

            diagParserTask.Wait();
            ConsoleNonLogReadFiles.Terminate();

            if (diagParserTask.IsFaulted || diagParserTask.IsCanceled)
            {
                ConsoleDisplay.End();

                GCMonitor.GetInstance().StopGCMonitoring();

                Logger.Instance.Info("DSEDiagnosticConsoleApplication Main End");

                ConsoleDisplay.Console.SetReWriteToWriterPosition();
                ConsoleDisplay.Console.WriteLine();
                Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                return;
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
        }
    }
}
