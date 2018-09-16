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
    public sealed class NodeExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Node;

        public NodeExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public NodeExcel(DataTable keyspaceDataTable,
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
                                                                                                        this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter));

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
                                                             workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                             workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                            //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                            workSheet.View.FreezePanes(3, 3);

                                                             this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                             this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);

                                                             this.DataTable.GetColumn("Rack");
                                                             this.DataTable.GetColumn("Status");
                                                             this.DataTable.GetColumn("Instance Type");
                                                             this.DataTable.GetColumn("Cluster Name");

                                                            //DataStax Versions
                                                            this.DataTable.SetGroupHeader("Versions", -2, true,
                                                                this.DataTable.GetColumn("DSE"),
                                                                this.DataTable.GetColumn("Cassandra"),
                                                                this.DataTable.GetColumn("Search"),
                                                                this.DataTable.GetColumn("Spark"),
                                                                this.DataTable.GetColumn("Agent"),
                                                                this.DataTable.GetColumn("Schema")
                                                            );

                                                             this.DataTable.GetColumn("VNodes");

                                                             this.DataTable.GetColumn("Storage Used (MB)")
                                                                            .SetNumericFormat("#,###,###,##0.00");
                                                             this.DataTable.GetColumn("Storage Utilization")
                                                                            .SetNumericFormat("##0.00%");
                                                             //this.DataTable.GetColumn("Health Rating")
                                                             //             .SetNumericFormat("0.00");

                                                             this.DataTable.GetColumn("Time Zone Offset");

                                                             this.DataTable.SetGroupHeader("Aggregated", -2, true,
                                                                this.DataTable.GetColumn("Start NodeTool Range")
                                                                                .SetCaption("Start")
                                                                                .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                this.DataTable.GetColumn("End NodeTool Range")
                                                                                .SetCaption("End")
                                                                                .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                this.DataTable.GetColumn("Uptime")
                                                                                .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat) 
                                                                 );

                                                                 this.DataTable.SetGroupHeader("System Log", -2, true,
                                                                    this.DataTable.GetColumn("Log Min Timestamp")
                                                                                    .SetCaption("Start")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("Log Max Timestamp")
                                                                                    .SetCaption("End")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("Log Duration")
                                                                                    .SetCaption("Duration")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat),
                                                                    this.DataTable.GetColumn("Log Timespan Difference")
                                                                                    .SetCaption("Gaps")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Log Nbr Files")
                                                                                    .SetCaption("Nbr Files")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn()
                                                                                );

                                                                this.DataTable.SetGroupHeader("Debug Log", -2, true,
                                                                    this.DataTable.GetColumn("Debug Log Min Timestamp")
                                                                                    .SetCaption("Start")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("Debug Log Max Timestamp")
                                                                                    .SetCaption("End")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("Debug Log Duration")
                                                                                    .SetCaption("Duration")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat),
                                                                    this.DataTable.GetColumn("Debug Log Timespan Difference")
                                                                                    .SetCaption("Gap")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Debug Log Nbr Files")
                                                                                    .SetCaption("Nbr Files")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn()
                                                                                    );

                                                                this.DataTable.GetColumn("Heap Memory (MB)")
                                                                                .SetNumericFormat("#,###,###,##0.00");
                                                                this.DataTable.GetColumn("Off Heap Memory (MB)")
                                                                                .SetNumericFormat("#,###,###,##0.00");
                                                                this.DataTable.GetColumn("Nbr VNodes")
                                                                                .SetNumericFormat("###");                                                                
                                                                this.DataTable.GetColumn("Percent Repaired")
                                                                                .SetNumericFormat("#,###,###,##0.0000%");
                                                                this.DataTable.GetColumn("Repair Service Enabled");
                                                                this.DataTable.GetColumn("Seed Node");
                                                                this.DataTable.GetColumn("Gossip Enabled");
                                                                this.DataTable.GetColumn("Thrift Enabled");
                                                                this.DataTable.GetColumn("Native Transport Enabled");
                                                                this.DataTable.GetColumn("Multi-Instance Server Id");
                                                                this.DataTable.GetColumn("Key Cache Information");
                                                                this.DataTable.GetColumn("Row Cache Information");
                                                                this.DataTable.GetColumn("Counter Cache Information");
                                                                this.DataTable.GetColumn("Chunk Cache Information");

                                                                 workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                 workSheet.ExcelRange(2,
                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress),
                                                                                       this.DataTable.GetColumn("Multi-Instance Server Id"))
                                                                             .First().AutoFilter = true;
                                                                                                                             
                                                                workSheet.AutoFitColumn();
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
