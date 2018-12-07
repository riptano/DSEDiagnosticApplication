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
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public CQLDLLExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
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
                                                                        {
                                                                            var workSheet = excelPackage.Workbook.Worksheets[WorkSheetName];

                                                                            workSheet.AltFileFillRow(3,
                                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace));

                                                                            this.CallActionEvent("Loaded");
                                                                        }
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
                                                                workSheet.View.FreezePanes(3, 5);

                                                                this.DataTable.GetColumn("Active")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonFalseRed);
                                                                //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace);
                                                                //this.DataTable.GetColumn("Name");
                                                                //this.DataTable.GetColumn("Type");
                                                                //this.DataTable.GetColumn("Partition Key");
                                                                //this.DataTable.GetColumn("Cluster Key");
                                                                //this.DataTable.GetColumn("Compaction Strategy");
                                                                //this.DataTable.GetColumn("Compression");

                                                                this.DataTable.SetGroupHeader("Read-Repair", -2, true,
                                                                    this.DataTable.GetColumn("Chance")
                                                                        .SetNumericFormat("0%")
                                                                        .SetComment("read_repair_chance -- Specifies the basis for invoking read repairs on reads in clusters. The value must be between 0 and 1. Default Values are: 0.0 (Cassandra 2.1, Cassandra 2.0.9 and later) 0.1 (Cassandra 2.0.8 and earlier)"),
                                                                    this.DataTable.GetColumn("DC Chance")
                                                                        .SetNumericFormat("0%")
                                                                        .SetComment("dclocal_read_repair_chance -- Specifies the probability of read repairs being invoked over all replicas in the current data center. Defaults are: 0.1 (Cassandra 2.1, Cassandra 2.0.9 and later) 0.0 (Cassandra 2.0.8 and earlier)"),
                                                                    this.DataTable.GetColumn("Policy")
                                                                    .SetComment("speculative_retry -- To override normal read timeout when read_repair_chance is not 1.0, sending another request to read, choose one of these values and use the property to create or alter the table: \"ALWAYS\" -- Retry reads of all replicas, \"Xpercentile\" -- Retry reads based on the effect on throughput and latency, \"Yms\" -- Retry reads after specified milliseconds, \"NONE\" -- Do not retry reads. Using the speculative retry property, you can configure rapid read protection in Cassandra 2.0.2 and later.Use this property to retry a request after some milliseconds have passed or after a percentile of the typical read latency has been reached, which is tracked per table.")
                                                                );

                                                                this.DataTable.SetGroupHeader("Tombstone", -2, true,
                                                                    this.DataTable.GetColumn("GC Grace Period")
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                        .SetComment("gc_grace_seconds -- Specifies the time to wait before garbage collecting tombstones (deletion markers). The default value allows a great deal of time for consistency to be achieved prior to deletion. In many deployments this interval can be reduced, and in a single-node cluster it can be safely set to zero. Default value is 864000 [10 days]"),
                                                                    this.DataTable.GetColumn("TTL")
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                );

                                                                this.DataTable.GetColumn("Memtable Flush Period")
                                                                    .SetNumericFormat("#,###,###,###")
                                                                    .SetComment("milliseconds");

                                                                this.DataTable.SetGroupHeader("Column Counts", -2, true,
                                                                    this.DataTable.GetColumn("Collections")
                                                                        .SetNumericFormat("#,###")
                                                                        .TotalColumn(),
                                                                    this.DataTable.GetColumn("Counters")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Blobs")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Static")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Frozen")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Tuple")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("UDT")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Total")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn()
                                                                );

                                                                this.DataTable.GetColumn("HasOrderBy")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTrueYellow);

                                                                this.DataTable.GetColumn("Storage (MB)")
                                                                    .SetNumericFormat("###,###,###,###.0000")
                                                                    .TotalColumn()
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue);

                                                                workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                workSheet.ExcelRange(2,
                                                                                      this.DataTable.GetColumn("Active"),
                                                                                      this.DataTable.GetColumn("Storage (MB)"))
                                                                            .First().AutoFilter = true;

                                                                workSheet.AutoFitColumn(workSheet.ExcelRange(this.DataTable.GetColumn("Active"),
                                                                                                                this.DataTable.GetColumn("Storage (MB)")));
                                                            },
                                                            -1,
                                                           -1,
                                                           "A2",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
