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
    public sealed class CQLDLLExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.CQLDLL;

        public CQLDLLExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public CQLDLLExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook)
            : this(keyspaceDataTable, excelTargetWorkbook, null, true)
        { }

        public override Tuple<IFilePath, string, int> Load()
        {
           var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, this.DataTable,
                                                           (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount, loadRange) =>
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
                                                                workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                                                workSheet.Cells["I1:K1"].Style.WrapText = true;
                                                                workSheet.Cells["I1:K1"].Merge = true;
                                                                workSheet.Cells["I1:K1"].Value = "Read-Repair";
                                                                workSheet.Cells["I1:I2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["K1:K2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                workSheet.Cells["N1:U1"].Style.WrapText = true;
                                                                workSheet.Cells["N1:U1"].Merge = true;
                                                                workSheet.Cells["N1:U1"].Value = "Column Counts";
                                                                workSheet.Cells["N1:N2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["U1:U2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                workSheet.View.FreezePanes(3, 1);
                                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "0%";
                                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "0%";
                                                                workSheet.Cells["L:L"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["M:M"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;

                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["T:T"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["U:U"].Style.Numberformat.Format = "#,###";

                                                                workSheet.Cells["A2:X2"].AutoFilter = true;

                                                                workSheet.Cells["I2"].AddComment("speculative_retry -- To override normal read timeout when read_repair_chance is not 1.0, sending another request to read, choose one of these values and use the property to create or alter the table: \"ALWAYS\" -- Retry reads of all replicas, \"Xpercentile\" -- Retry reads based on the effect on throughput and latency, \"Yms\" -- Retry reads after specified milliseconds, \"NONE\" -- Do not retry reads. Using the speculative retry property, you can configure rapid read protection in Cassandra 2.0.2 and later.Use this property to retry a request after some milliseconds have passed or after a percentile of the typical read latency has been reached, which is tracked per table.", "Richard Andersen");
                                                                workSheet.Cells["J2"].AddComment("dclocal_read_repair_chance -- Specifies the probability of read repairs being invoked over all replicas in the current data center. Defaults are: 0.1 (Cassandra 2.1, Cassandra 2.0.9 and later) 0.0 (Cassandra 2.0.8 and earlier)", "Richard Andersen");
                                                                workSheet.Cells["K2"].AddComment("read_repair_chance -- Specifies the basis for invoking read repairs on reads in clusters. The value must be between 0 and 1. Default Values are: 0.0 (Cassandra 2.1, Cassandra 2.0.9 and later) 0.1 (Cassandra 2.0.8 and earlier)", "Richard Andersen");
                                                                workSheet.Cells["L2"].AddComment("gc_grace_seconds -- Specifies the time to wait before garbage collecting tombstones (deletion markers). The default value allows a great deal of time for consistency to be achieved prior to deletion. In many deployments this interval can be reduced, and in a single-node cluster it can be safely set to zero. Default value is 864000 [10 days]", "Richard Andersen");
                                                                
                                                                workSheet.AutoFitColumn(workSheet.Cells["A:X"]);
                                                            },
                                                            -1,
                                                           -1,
                                                           "A2",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
