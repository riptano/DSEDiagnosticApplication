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
    public sealed class DataCenterExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.DataCenters;

        public DataCenterExcel(DataTable dcDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(dcDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public DataCenterExcel(DataTable keyspaceDataTable,
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

                                                                             workSheet.AltFileFillRow(4,
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
                                                                 workSheet.Cells["1:3"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:3"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                                                 this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter)
                                                                                  .SetCaption("Name")
                                                                                  .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDCNodeStatus);

                                                                 this.DataTable.SetGroupHeader("Total", -1, false,
                                                                    this.DataTable.GetColumn("Total Nodes")
                                                                                     .SetCaption("Nodes")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn());

                                                                 this.DataTable.SetGroupHeader("Workload Types (Nodes)", -2, true,
                                                                     this.DataTable.GetColumn("Cassandra")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Search")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Analytics")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Graph")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Multi-Instance")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Other")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn()
                                                                 );

                                                                 this.DataTable.SetGroupHeader("Status (Nodes)", -2, true,
                                                                     this.DataTable.GetColumn("Status Up")
                                                                                     .SetCaption("Up")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn(),
                                                                     this.DataTable.GetColumn("Status Down")
                                                                                     .SetCaption("Down")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn()
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusDown),
                                                                     this.DataTable.GetColumn("Status Unknown")
                                                                                     .SetCaption("Unknown")
                                                                                     .SetNumericFormat("#,###,###,##0")
                                                                                     .TotalColumn()
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonNodeNbrStatusUnkown)
                                                                     );

                                                                 this.DataTable.SetGroupHeader("DSE", -2, true,
                                                                     this.DataTable.GetColumn("DSE Version")
                                                                                         .SetCaption("Version")                                                                                         
                                                                                     );

                                                                //Device Utilization 
                                                                this.DataTable.SetGroupHeader("DSE Storage Utilization", -2, true,
                                                                    this.DataTable.GetColumn("DU Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                    this.DataTable.GetColumn("DU Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                    this.DataTable.GetColumn("DU Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                    this.DataTable.GetColumn("DU Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                    this.DataTable.GetColumn("DU Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization)
                                                                    );

                                                                this.DataTable.SetGroupHeader("Insufficient Space", -1, true,
                                                                   this.DataTable.GetColumn("DU Insufficient Space")
                                                                                   .SetCaption("Warnings")
                                                                                   .SetNumericFormat("#,###,###")
                                                                                   .TotalColumn()
                                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonInsufficientSpace)
                                                                                   );

                                                               //Storage
                                                               this.DataTable.SetGroupHeader("Storage (MB)", -2, true,
                                                                    this.DataTable.GetColumn("Storage Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("#,###,###,##0.0000")
                                                                                     .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorage),
                                                                    this.DataTable.GetColumn("Storage Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                    this.DataTable.GetColumn("Storage Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("#,###,###,##0.0000")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorage), 
                                                                    this.DataTable.GetColumn("Storage Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                    this.DataTable.GetColumn("Storage Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("#,###,###,##0.0000")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorage),
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
                                                                                    .SetCaption("Percent-Cluster")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                );

                                                                //SSTables
                                                                this.DataTable.SetGroupHeader("SSSTables", -2, true,
                                                                   this.DataTable.GetColumn("SSTables Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTables),
                                                                   this.DataTable.GetColumn("SSTables Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                   this.DataTable.GetColumn("SSTables Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTables),
                                                                   this.DataTable.GetColumn("SSTables Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                   this.DataTable.GetColumn("SSTables Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("#,###,###,##0.00")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTables),
                                                                   this.DataTable.GetColumn("SSTables StdDev")
                                                                                    .SetCaption("StdDev")
                                                                                    .SetNumericFormat("#,###,###,##0.00"),
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
                                                                                    .SetCaption("Percent-Cluster")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                                    );
                                                                //Distribution
                                                                this.DataTable.SetGroupHeader("Distribution", -2, true,
                                                                    this.DataTable.SetGroupHeader("Storage", -1, true,
                                                                       this.DataTable.GetColumn("Distribution Storage From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Storage To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Storage StdDev")
                                                                                        .SetCaption("StdDev")
                                                                                        .SetNumericFormat("###,##0.00")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDistStdDev)
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("Keys", -1, true,
                                                                       this.DataTable.GetColumn("Distribution Keys From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Keys To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Keys StdDev")
                                                                                        .SetCaption("StdDev")
                                                                                        .SetNumericFormat("###,##0.00")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDistStdDev)
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("SSTables", -1, true,
                                                                       this.DataTable.GetColumn("Distribution SSTables From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution SSTables To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution SSTables StdDev")
                                                                                        .SetCaption("StdDev")
                                                                                        .SetNumericFormat("###,##0.00")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDistStdDev)
                                                                ));

                                                                 this.DataTable.GetColumn("Active Tbls/Idxs/Vws")
                                                                    .SetNumericFormat("##0%")
                                                                    .SetComment("Active User Tables, Indexes, and Views percent")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonActiveTblsPercent);

                                                                //Counts
                                                                this.DataTable.SetGroupHeader("Cell Counts", -2, true,
                                                                    this.DataTable.SetGroupHeader("Read", -1, true,
                                                                       this.DataTable.GetColumn("Reads Max")
                                                                                        .SetCaption("Max")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Reads Max-Node")
                                                                                        .SetCaption("Max-Node"),//am
                                                                       this.DataTable.GetColumn("Reads Min")
                                                                                        .SetCaption("Min")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Reads Min-Node")
                                                                                        .SetCaption("Min-Node"),
                                                                       this.DataTable.GetColumn("Reads Avg")
                                                                                        .SetCaption("Avg")
                                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                       this.DataTable.GetColumn("Reads StdDev")
                                                                                        .SetCaption("StdDev")
                                                                                        .SetNumericFormat("#,###,###,##0.00"),
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
                                                                                        .SetCaption("Percent-Cluster")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)                                                                                    
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("Write", -1, true,
                                                                       this.DataTable.GetColumn("Writes Max")
                                                                                        .SetCaption("Max")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Writes Max-Node")
                                                                                        .SetCaption("Max-Node"),
                                                                       this.DataTable.GetColumn("Writes Min")
                                                                                        .SetCaption("Min")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Writes Min-Node")
                                                                                        .SetCaption("Min-Node"),
                                                                       this.DataTable.GetColumn("Writes Avg")
                                                                                        .SetCaption("Avg")
                                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                       this.DataTable.GetColumn("Writes StdDev")
                                                                                        .SetCaption("StdDev")
                                                                                        .SetNumericFormat("#,###,###,##0.00"),
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
                                                                                        .SetCaption("Percent-Cluster")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarGreenYellow)
                                                                 ));

                                                                this.DataTable.SetGroupHeader(" ", -1, true,
                                                                       this.DataTable.GetColumn("Batch Percent")                                                                                        
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("LWT Percent")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        .TotalColumn()
                                                                                        );

                                                                 this.DataTable.SetGroupHeader("Average", -1, true,
                                                                        this.DataTable.GetColumn("Node UpTime")
                                                                            .AvgerageColumn(includeCondFmt: false)
                                                                            .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration),
                                                                        this.DataTable.GetColumn("Log System Duration")
                                                                            .AvgerageColumn(includeCondFmt: false)
                                                                            .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration),
                                                                        this.DataTable.GetColumn("Log Debug Duration")
                                                                            .AvgerageColumn(includeCondFmt: false)
                                                                            .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonAnalysisDuration)
                                                                        );

                                                                 workSheet.UpdateWorksheet(this.DataTable, 3);

                                                                workSheet.View.FreezePanes(4, 2);
                                                                workSheet.ExcelRange(3,
                                                                                      this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter),
                                                                                      this.DataTable.GetColumn("Log Debug Duration"))
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
