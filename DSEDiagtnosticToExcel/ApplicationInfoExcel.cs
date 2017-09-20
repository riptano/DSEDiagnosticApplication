using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;

namespace DSEDiagtnosticToExcel
{
    public sealed class ApplicationInfoExcel : LoadToExcel
    {
        public const string DataTableName = "Application";

        public sealed class ApplInfo
        {
            public string ApplicationName;
            public string ApplicationVersion;
            public string ApplicationAssemblyDir;
            public string WorkingDir;
            public DateTimeRange ApplicationStartEndTime;
            public string ApplicationArgs;
            public string ApplicationLibrarySettings;
            public string DiagnosticDirectory;
            public bool Aborted;
            public int Erroes;
            public int Warnings;

            public sealed class ResultInfo
            {
                public string MapperClass;
                public int MapperId;
                public string Catagory;
                public string DataCenter;
                public string Node;
                public int NbrTasksCompleted;
                public int NbrItemsParsed;
                public int NbrItemsGenerated;
                public int NbrTasksCanceled;
                public int NbrExceptions;
            }

            public IList<ResultInfo> Results { get; } = new List<ResultInfo>();
        }

        public ApplicationInfoExcel(IFilePath excelTargetWorkbook,
                                        string worksheetName)
            : base(new DataTable(DataTableName, DSEDiagnosticToDataTable.TableNames.Namespace),
                    excelTargetWorkbook,
                    worksheetName,
                    true)
        { }

        public ApplicationInfoExcel(IFilePath excelTargetWorkbook)
            : this(excelTargetWorkbook,
                    null)
        { }

        public ApplInfo ApplicationInfo { get; } = new ApplInfo();

        public override Tuple<IFilePath, string, int> Load()
        {
            bool hasException = false;

            #region Update DataTable with ResultInfo
            this.DataTable.Columns.Add("Mapper Class", typeof(string));//a
            this.DataTable.Columns.Add("Mapper Id", typeof(int));//b
            this.DataTable.Columns.Add("Catagory", typeof(string));
            this.DataTable.Columns.Add("Data Center", typeof(string)).AllowDBNull = true; //d
            this.DataTable.Columns.Add("Node", typeof(string)).AllowDBNull = true; //e
            this.DataTable.Columns.Add("Nbr Tasks Completed", typeof(int));
            this.DataTable.Columns.Add("Nbr Items Parsed", typeof(int));
            this.DataTable.Columns.Add("Nbr Items Generated", typeof(int)); //h
            this.DataTable.Columns.Add("Nbr Tasks Canceled", typeof(int));
            this.DataTable.Columns.Add("Nbr Exceptions", typeof(int));//j

            foreach (var item in this.ApplicationInfo.Results)
            {
                var dataRow = this.DataTable.NewRow();

                dataRow.SetField("Mapper Class", item.MapperClass);
                dataRow.SetField("Mapper Id", item.MapperId);
                dataRow.SetField("Catagory", item.Catagory);
                dataRow.SetField("Data Center", item.DataCenter);
                dataRow.SetField("Node", item.Node);
                dataRow.SetField("Nbr Tasks Completed", item.NbrTasksCompleted);
                dataRow.SetField("Nbr Items Parsed", item.NbrItemsParsed);
                dataRow.SetField("Nbr Items Generated", item.NbrItemsGenerated);
                dataRow.SetField("Nbr Tasks Canceled", item.NbrTasksCanceled);
                dataRow.SetField("Nbr Exceptions", item.NbrExceptions);

                hasException = item.NbrExceptions > 0;

                this.DataTable.Rows.Add(dataRow);
            }

            #endregion

            var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, this.DataTable,
                                                           (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount) =>
                                                           {
                                                               switch (stage)
                                                               {
                                                                   case WorkBookProcessingStage.PreProcess:
                                                                   case WorkBookProcessingStage.PrepareFileName:
                                                                   case WorkBookProcessingStage.PreProcessDataTable:
                                                                       break;
                                                                   case WorkBookProcessingStage.PreLoad:
                                                                       this.CallActionEvent("Begin Loading");
                                                                       break;
                                                                   case WorkBookProcessingStage.PreSave:
                                                                       this.CallActionEvent("Loaded");
                                                                       break;
                                                                   case WorkBookProcessingStage.Saved:
                                                                       this.CallActionEvent("Workbook Saved");
                                                                       break;
                                                                   case WorkBookProcessingStage.PostProcess:
                                                                       break;
                                                                   default:
                                                                       break;
                                                               }
                                                           },
                                                            workSheet =>
                                                            {

                                                                workSheet.Column(1).Width = 50;
                                                                workSheet.Column(1).Style.WrapText = true;
                                                                workSheet.Column(1).Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;

                                                                if (!this.AppendToWorkSheet)
                                                                {
                                                                    workSheet.Cells["B12"].Value =
                                                                        workSheet.Cells["A2"].Value = this.ApplicationInfo.Aborted ? "** Aborted **" : (hasException ? "** Exception(s) Detected **" : null);
                                                                    workSheet.Cells["A3"].Value = this.ApplicationInfo.ApplicationName;
                                                                    workSheet.Cells["A4"].Value = this.ApplicationInfo.ApplicationVersion;
                                                                    workSheet.Cells["A5"].Value = this.ApplicationInfo.ApplicationAssemblyDir;
                                                                    workSheet.Cells["A6"].Value = "Application Arguments:\r\n\t"
                                                                                                    + this.ApplicationInfo.ApplicationArgs.Replace(" -", "\r\n\t-");
                                                                    workSheet.Cells["A7"].Value = "Library Settings:\r\n"
                                                                                                    + this.ApplicationInfo.ApplicationLibrarySettings;
                                                                    if (this.ApplicationInfo.ApplicationStartEndTime != null)
                                                                    {
                                                                        workSheet.Cells["A8"].Value = string.Format("Started: {0:yyyy-MM-dd\\ HH\\:mm\\:ss}\r\nEnded: {1:yyyy-MM-dd\\ HH\\:mm\\:ss}\r\nDuration: {2:d\\ hh\\:mm}",
                                                                                                                    this.ApplicationInfo.ApplicationStartEndTime.Min,
                                                                                                                    this.ApplicationInfo.ApplicationStartEndTime.Max,
                                                                                                                    this.ApplicationInfo.ApplicationStartEndTime.TimeSpan());
                                                                    }
                                                                    workSheet.Cells["A9"].Value = "Working Directory: " + this.ApplicationInfo.WorkingDir;
                                                                    workSheet.Cells["A10"].Value = "Diagnostic Directory: " + this.ApplicationInfo.DiagnosticDirectory;
                                                                    workSheet.Cells["A11"].Value = string.Format("Errors: {0:###,###,##0} Warnings: {1:###,###,##0}", this.ApplicationInfo.Erroes, this.ApplicationInfo.Warnings);

                                                                    if(this.ApplicationInfo.Erroes > 0)
                                                                    {
                                                                        workSheet.Cells["D12"].Value = "** Errors Detected in Application Log **";
                                                                    }

                                                                    for (int nRow = 1; nRow <= 11; ++nRow)
                                                                    {
                                                                        workSheet.Row(nRow).OutlineLevel = 1;
                                                                        workSheet.Row(nRow).Collapsed = true;
                                                                    }
                                                                    workSheet.Column(1).OutlineLevel = 1;
                                                                    workSheet.Column(1).Collapsed = true;
                                                                }

                                                                //DataTable
                                                                workSheet.Cells["13:13"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["13:13"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workSheet.View.FreezePanes(3, 1);

                                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0";

                                                                workSheet.Cells["B13:K13"].AutoFilter = true;

                                                                workSheet.AutoFitColumn(workSheet.Cells["B:K"]);
                                                            },
                                                            -1,
                                                           -1,
                                                           "B13",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           clearWorkSheet: false);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
