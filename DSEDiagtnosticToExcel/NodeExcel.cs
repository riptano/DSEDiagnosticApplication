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
                                                             (workSheet, splitNbr) =>
                                                             {
                                                                 workSheet.Cells["1:3"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:3"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                            
                                                                //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                                //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);

                                                                //this.DataTable.GetColumn("Rack");
                                                                this.DataTable.GetColumn("Status")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeStatus);
                                                                //this.DataTable.GetColumn("Instance Type");
                                                                //this.DataTable.GetColumn("Cluster Name");

                                                                //DataStax Versions
                                                                this.DataTable.SetGroupHeader("Versions", -2, true,
                                                                    this.DataTable.GetColumn("DSE"),
                                                                    this.DataTable.GetColumn("Cassandra"),
                                                                    this.DataTable.GetColumn("Search"),
                                                                    this.DataTable.GetColumn("Spark"),
                                                                    this.DataTable.GetColumn("Agent"),
                                                                    this.DataTable.GetColumn("Schema")
                                                                );

                                                                this.DataTable.SetGroupHeader("Status", -2, true,                                                                    
                                                                    this.DataTable.SetGroupHeader("Detected Occurrences", -1, true,
                                                                            this.DataTable.GetColumn("Status Starts")
                                                                                        .SetCaption("Starts")
                                                                                        .SetComment("Start only occurrences, no corresponding shutdown")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Status Restarts")
                                                                                        .SetCaption("ReStart")
                                                                                        .SetComment("Corresponding shutdown and restart occurrences")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Long Down Pause")
                                                                                        .SetCaption("Long Duration")
                                                                                        .SetComment("Number of occurrences where the amount of time between events exceed a threshold (default is 4 hours)")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusDown)
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Status Not Responding")
                                                                                        .SetCaption("Not Responding")
                                                                                        .SetComment("Number of occurrences where nodes were not responding")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusDown)
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Status Dead")
                                                                                        .SetCaption("Dead")
                                                                                        .SetComment("Number of occurrences where nodes were detected as \"Dead\"")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusDown)
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Network Events")
                                                                                        .SetCaption("Network")
                                                                                        .SetComment("Number of Possible Network Related Event")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusDown)
                                                                                        .TotalColumn(),
                                                                            this.DataTable.GetColumn("Status Other")
                                                                                        .SetCaption("Other")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn())
                                                                        );

                                                                 this.DataTable.SetGroupHeader("DSE Storage Utilization", -2, true,
                                                                    this.DataTable.GetColumn("Device Utilized")
                                                                                    .SetCaption("Utilized")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                    this.DataTable.GetColumn("DU Insufficient Space")
                                                                                        .SetCaption("Warnings")
                                                                                        .SetNumericFormat("#,###,###")
                                                                                        .TotalColumn()
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonInsufficientSpace)
                                                                                    
                                                                    );

                                                                 //Storage
                                                                 this.DataTable.SetGroupHeader("Storage (MB)", -2, true,
                                                                      this.DataTable.GetColumn("Storage Total")
                                                                                      .SetCaption("Total")
                                                                                      .SetNumericFormat("#,###,###,##0.0000")
                                                                                      .TotalColumn()
                                                                                      .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                      this.DataTable.GetColumn("Storage Total (User)")
                                                                                      .SetCaption("Total (User)")
                                                                                      .SetNumericFormat("#,###,###,##0.0000")
                                                                                      .TotalColumn()
                                                                                      .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                      this.DataTable.GetColumn("Storage Percent")
                                                                                      .SetCaption("Percent-DC")
                                                                                      .SetNumericFormat("##0.00%")
                                                                                      .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                  );

                                                                 //SSTables
                                                                 this.DataTable.SetGroupHeader("SSSTables", -2, true,                                                                    
                                                                    this.DataTable.GetColumn("SSTables Total")
                                                                                     .SetCaption("Total")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn()
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                    this.DataTable.GetColumn("SSTables Total (User)")
                                                                                     .SetCaption("Total (User)")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn()
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn("SSTables Percent")
                                                                                     .SetCaption("Percent-DC")
                                                                                     .SetNumericFormat("##0.00%")
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                     );

                                                                 this.DataTable.SetGroupHeader("Cell Counts", -2, true,
                                                                    this.DataTable.SetGroupHeader("Read", -1, true,                                                                       
                                                                       this.DataTable.GetColumn("Reads Total")
                                                                                        .SetCaption("Total")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn()
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                       this.DataTable.GetColumn("Reads Total (User)")
                                                                                        .SetCaption("Total (User)")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn()
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                       this.DataTable.GetColumn("Reads Percent")
                                                                                        .SetCaption("Percent-DC")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("Write", -1, true,                                                                       
                                                                       this.DataTable.GetColumn("Writes Total")
                                                                                        .SetCaption("Total")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn()
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                       this.DataTable.GetColumn("Writes Total (User)")
                                                                                        .SetCaption("Total (User)")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn()
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                       this.DataTable.GetColumn("Writes Percent")
                                                                                        .SetCaption("Percent-DC")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                 ));

                                                                 this.DataTable.SetGroupHeader("Instance Counts", -2, true,
                                                                     this.DataTable.SetGroupHeader("Log", -1, true,
                                                                        this.DataTable.GetColumn("Log Event Total")
                                                                                         .SetCaption("Events")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                        this.DataTable.GetColumn("Log Event Percent")
                                                                                         .SetCaption("Percent-DC")
                                                                                         .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                         ),
                                                                     this.DataTable.SetGroupHeader("GC", -1, true,
                                                                        this.DataTable.GetColumn("GC Events Total")
                                                                                         .SetCaption("Occurrences")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                        this.DataTable.GetColumn("GC Events Percent")
                                                                                         .SetCaption("Percent-DC")
                                                                                         .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                         ),
                                                                      this.DataTable.SetGroupHeader("Flush", -1, true,
                                                                        this.DataTable.GetColumn("Flush Total")
                                                                                         .SetCaption("Total")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                        this.DataTable.GetColumn("Flush Total (User)")
                                                                                         .SetCaption("Total (User)")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                        this.DataTable.GetColumn("Flush Percent")
                                                                                         .SetCaption("Percent-DC")
                                                                                         .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                         ),
                                                                      this.DataTable.SetGroupHeader("Compaction", -1, true,
                                                                        this.DataTable.GetColumn("Compaction Total")
                                                                                         .SetCaption("Total")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                        this.DataTable.GetColumn("Compaction Total (User)")
                                                                                         .SetCaption("Total (User)")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                        this.DataTable.GetColumn("Compaction Percent")
                                                                                         .SetCaption("Percent-DC")
                                                                                         .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                         ),
                                                                      this.DataTable.SetGroupHeader("Repairs", -1, true,
                                                                        this.DataTable.GetColumn("Repair Total")
                                                                                         .SetCaption("Total")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .TotalColumn()
                                                                                         .SetComment("Includes NodeSync and any non-keyspace based repairs")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen),
                                                                        this.DataTable.GetColumn("Repair Total (User)")
                                                                                         .SetCaption("Total (User)")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .SetComment("Only application keyspaces (no NodeSync activities)")
                                                                                         .TotalColumn()
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                        this.DataTable.GetColumn("Repair Percent")
                                                                                         .SetCaption("Percent-DC")
                                                                                         .SetNumericFormat("##0.00%")
                                                                                         .SetComment("All types of repairs (keyspace/non-keyspace) for whole cluster")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow),
                                                                      this.DataTable.GetColumn("Repair Est Tasks (User)")
                                                                                         .SetCaption("Est. Tasks (User)")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .SetComment("The estimated number of tasks to complete a full repair for User Keyspaces")
                                                                                         .TotalColumn(),
                                                                      this.DataTable.GetColumn("Node Sync Tables (User)")
                                                                                         .SetCaption("NodeSync Tables (User)")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .SetComment("The number of User/Application tables where NodeSync is enabled within a DC.")
                                                                                         .TotalColumn(),
                                                                      this.DataTable.GetColumn("Node Sync Count")
                                                                                         .SetCaption("NodeSync Counts")
                                                                                         .SetNumericFormat("#,###,###,##0")
                                                                                         .SetComment("The estimated number of NodeSync cell operations (read/write)")
                                                                                         .TotalColumn()
                                                                        )
                                                                    );


                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                       this.DataTable.GetColumn("Batch Percent")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("LWT Percent")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .TotalColumn()
                                                                                        );

                                                                 //this.DataTable.GetColumn("Time Zone Offset");

                                                                 this.DataTable.SetGroupHeader("Aggregated", -2, true,
                                                                    this.DataTable.GetColumn("Start NodeTool Range")
                                                                                    .SetCaption("Start")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("End NodeTool Range")
                                                                                    .SetCaption("End")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat),
                                                                    this.DataTable.GetColumn("Uptime")
                                                                                    .AvgerageColumn(includeCondFmt: false)
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration)
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
                                                                                    .AvgerageColumn(includeCondFmt: false)
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration),
                                                                    this.DataTable.GetColumn("Log Timespan Difference")
                                                                                    .SetCaption("Gaps")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .TotalColumn()
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisLogGap),
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
                                                                                    .AvgerageColumn(includeCondFmt: false)
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration),
                                                                    this.DataTable.GetColumn("Debug Log Timespan Difference")
                                                                                    .SetCaption("Gap")
                                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                                    .TotalColumn()
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisLogGap),
                                                                    this.DataTable.GetColumn("Debug Log Nbr Files")
                                                                                    .SetCaption("Nbr Files")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn()
                                                                                    );

                                                                 //this.DataTable.GetColumn("VNodes");
                                                                 this.DataTable.GetColumn("Nbr VNodes")
                                                                                .SetNumericFormat("###");                                                                
                                                                
                                                                //this.DataTable.GetColumn("Repair Service Enabled");
                                                                //this.DataTable.GetColumn("Seed Node");
                                                                //this.DataTable.GetColumn("Gossip Enabled");
                                                                //this.DataTable.GetColumn("Thrift Enabled");
                                                                //this.DataTable.GetColumn("Native Transport Enabled");
                                                                //this.DataTable.GetColumn("Multi-Instance Server Id");
                                                                //this.DataTable.GetColumn("Key Cache Information");
                                                                //this.DataTable.GetColumn("Row Cache Information");
                                                                //this.DataTable.GetColumn("Counter Cache Information");
                                                                //this.DataTable.GetColumn("Chunk Cache Information");

                                                                 workSheet.UpdateWorksheet(this.DataTable, 3);
                                                                 workSheet.View.FreezePanes(4, 3);
                                                                 workSheet.ExcelRange(3,
                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress),
                                                                                       this.DataTable.GetColumn("Multi-Instance Server Id"))
                                                                             .First().AutoFilter = true;
                                                                                                                             
                                                                workSheet.AutoFitColumn();
                                                            },
                                                            -1,
                                                           -1,
                                                           "A3",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
