﻿using System;
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
        static Task<IFilePath> LoadExcelWorkbook (Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> diagParserTask,
                                                    Task<System.Data.DataSet> loadAllDataTableTask,
                                                    System.Threading.CancellationTokenSource cancellationSource)
        {
            if (ParserSettings.ExcelFilePath == null)
            {
                var cluster = DSEDiagnosticLibrary.Cluster.Clusters.FirstOrDefault(c => !c.IsMaster) ?? DSEDiagnosticLibrary.Cluster.Clusters.First();

                if (cluster.IsMaster)
                {
                    Logger.Instance.Debug("Load Excel is waiting on non-Master cluster");
                    ConsoleExcelWorkbook.Increment("Load Excel is waiting on non-Master cluster");
                    Common.Patterns.Threading.LockFree.SpinWait(() =>
                    {
                        System.Threading.Thread.Sleep(500);
                        return !(cluster = DSEDiagnosticLibrary.Cluster.Clusters.FirstOrDefault(c => !c.IsMaster) ?? DSEDiagnosticLibrary.Cluster.Clusters.First()).IsMaster;
                    });

                    ConsoleExcelWorkbook.TaskEnd("Load Excel is waiting on non-Master cluster");
                    Logger.Instance.DebugFormat("Load Excel is using Cluster \"{0}\"", cluster.Name);
                }

                ParserSettings.ExcelFilePath = Common.Path.PathUtils.BuildFilePath(string.Format(Properties.Settings.Default.ExcelFileNameGeneratedStringFormat,
                                                                                                    ParserSettings.DiagnosticPath,
                                                                                                    cluster.IsMaster ? "MasterCluster" : cluster.Name,
                                                                                                    RunDateTime,
                                                                                                    ParserSettings.ExcelFileTemplatePath?.FileExtension ?? DSEDiagtnosticToExcel.LibrarySettings.ExcelFileExtension));
            }

            var loadExcel = new DSEDiagtnosticToExcel.LoadDataSet(loadAllDataTableTask,
                                                                        ParserSettings.ExcelFilePath,
                                                                        ParserSettings.ExcelFileTemplatePath,
                                                                        cancellationSource);

            loadExcel.OnAction += (DSEDiagtnosticToExcel.IExcel sender, string action) =>
            {
                if (action == "Begin Loading")
                {
                    if (sender.LoadTo == DSEDiagtnosticToExcel.LoadToTypes.WorkBook)
                    {
                        ConsoleExcelWorkbook.Increment(sender.ExcelTargetWorkbook);
                    }
                    else
                    {
                        ConsoleExcelWorkSheet.Increment(sender.WorkSheetName);
                    }
                }
                else if (action == "Loaded")
                {
                    if (sender.LoadTo == DSEDiagtnosticToExcel.LoadToTypes.WorkBook)
                    {
                        ConsoleExcelWorkbook.TaskEnd(sender.ExcelTargetWorkbook);
                    }
                    else
                    {
                        ConsoleExcelWorkSheet.TaskEnd(sender.WorkSheetName);
                    }
                }
                else if (action == "Workbook Saved")
                {
                    ConsoleExcelWorkbook.Increment(sender.ExcelTargetWorkbook);
                }
            };

            var excelTask = loadExcel.Load();
            var excelUpdateAppInfo = UpdateExcelApplInfoWorksheet(diagParserTask, excelTask, cancellationSource);

            excelUpdateAppInfo.Then(result => { ConsoleExcelWorkbook.Terminate(); ConsoleExcelWorkSheet.Terminate(); });
            excelUpdateAppInfo.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                                TaskContinuationOptions.OnlyOnFaulted);
            excelUpdateAppInfo.ContinueWith(task => CanceledFaultProcessing(null),
                                                TaskContinuationOptions.OnlyOnCanceled);

            return excelUpdateAppInfo;
        }

        static Task<IFilePath> UpdateExcelApplInfoWorksheet(Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> diagParserTask,
                                                            Task<IFilePath> excelTask,
                                                            System.Threading.CancellationTokenSource cancellationSource)
        {
            return Task.Factory.ContinueWhenAll(new Task[] { diagParserTask, excelTask }, tasks =>
            {
                var parsedResults = ((Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>>)tasks[0]).Result;
                var excelResults = ((Task<IFilePath>)tasks[1]).Result;

                var loadAppInfo = new DSEDiagtnosticToExcel.ApplicationInfoExcel(excelResults);

                loadAppInfo.ApplicationInfo.Aborted = AlreadyCanceled || tasks.Any(t => t.IsCanceled);
                loadAppInfo.ApplicationInfo.Errors = (int)ConsoleErrors.Counter;
                loadAppInfo.ApplicationInfo.Warnings = (int)ConsoleWarnings.Counter;
                loadAppInfo.ApplicationInfo.ApplicationArgs = CommandLineArgsString;
                loadAppInfo.ApplicationInfo.ApplicationAssemblyDir = Common.Functions.Instance.AssemblyDir;
                loadAppInfo.ApplicationInfo.ApplicationLibrarySettings = ParserSettings.SettingValues();
                loadAppInfo.ApplicationInfo.ApplicationName = Common.Functions.Instance.ApplicationName;
                loadAppInfo.ApplicationInfo.ApplicationStartEndTime = new DateTimeRange(RunDateTime, DateTime.Now);
                loadAppInfo.ApplicationInfo.ApplicationVersion = Common.Functions.Instance.ApplicationVersion;
                loadAppInfo.ApplicationInfo.DiagnosticDirectory = ParserSettings.DiagnosticPath.PathResolved;
                loadAppInfo.ApplicationInfo.WorkingDir = Common.Functions.Instance.ApplicationRunTimeDir;

                var resultItems = from result in parsedResults
                                  group result by new { DC = result.Node?.DataCenter?.Name, Node = result.Node?.Id.NodeName(), Class = result.GetType().Name, Category = result.Catagory, MapperId = result.MapperId } into g
                                  select new
                                  {
                                      DC = g.Key.DC,
                                      Node = g.Key.Node,
                                      Class = g.Key.Class,
                                      Category = g.Key.Category.ToString(),
                                      MapperId = g.Key.MapperId,
                                      NbrTasksCompleted = g.Count(i => i.Processed),
                                      NbrItemsParsed = g.Sum(i => i.NbrItemsParsed),
                                      NbrItemsGenerated = g.Sum(i => i.NbrItemGenerated),
                                      NbrTasksCanceled = g.Count(i => i.Canceled),
                                      NbrWarnings = g.Sum(i => i.NbrWarnings),
                                      NbrErrors = g.Sum(i => i.NbrErrors),
                                      NbrExceptions = g.Count(i => i.Exception != null || (i.ExceptionStrings?.HasAtLeastOneElement() ?? false))
                                  };

                foreach (var item in resultItems)
                {
                    loadAppInfo.ApplicationInfo.Results.Add(new DSEDiagtnosticToExcel.ApplicationInfoExcel.ApplInfo.ResultInfo()
                    {
                        Catagory = item.Category,
                        MapperClass = item.Class,
                        DataCenter = item.DC,
                        MapperId = item.MapperId,
                        NbrExceptions = item.NbrExceptions,
                        NbrErrors = item.NbrErrors,
                        NbrWarnings = item.NbrWarnings,
                        NbrItemsGenerated = item.NbrItemsGenerated,
                        NbrItemsParsed = item.NbrItemsParsed,
                        NbrTasksCanceled = item.NbrTasksCanceled,
                        NbrTasksCompleted = item.NbrTasksCompleted,
                        Node = item.Node
                    });
                }
                return loadAppInfo.Load()?.Item1;
            },
            cancellationSource.Token,
            TaskContinuationOptions.OnlyOnRanToCompletion,
            TaskScheduler.Current);
        }
    }
}