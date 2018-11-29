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
    public sealed class KeyspaceExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Keyspaces;

        public KeyspaceExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public KeyspaceExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        {
        }

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
                                                                                                        this.DataTable.GetColumn("Name"),
                                                                                                        null,
                                                                                                        this.DataTable.GetColumn("Total"),
                                                                                                        this.DataTable.GetColumn("Tables"),
                                                                                                        this.DataTable.GetColumn("DDL"));

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
                                                                 var dtKeySpace = this.DataTable;

                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                                                 this.DataTable.GetColumn("Name")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonKeySpaceNameActivite);
                                                                 this.DataTable.SetGroupHeader("Replication", -1, true,
                                                                     this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter),
                                                                     this.DataTable.GetColumn("Replication Strategy"),
                                                                     this.DataTable.GetColumn("Replication Factor").SetNumericFormat("#,###")
                                                                  );
                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                     this.DataTable.GetColumn("Tables").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("Views").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("Secondary Indexes").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("solr Indexes").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("SAS Indexes").SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn("Custom Indexes").SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn("Functions").SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn("Triggers").SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn("Total").SetNumericFormat("#,##0").TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTotTbls),
                                                                     this.DataTable.GetColumn("Active").SetNumericFormat("#,##0").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonActiveTbls)
                                                                     );
                                                                 this.DataTable.SetGroupHeader("Compaction Strategy", -1, true,
                                                                     this.DataTable.GetColumn("STCS").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("LCS").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("DTCS").SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn("TCS").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("TWCS").SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn("Other Strategies").SetNumericFormat("#,###").TotalColumn()
                                                                  );
                                                                 this.DataTable.SetGroupHeader("Columns", -1, true,
                                                                    this.DataTable.GetColumn("Column Total").SetCaption("Total").SetNumericFormat("#,##0").TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTotTbls),
                                                                    this.DataTable.GetColumn("Column Collections").SetCaption("Collections").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn("Column Blobs").SetCaption("Blobs").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn("Column Static").SetCaption("Statics").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn("Column Frozen").SetCaption("Frozen").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn("Column Tuple").SetCaption("Tuples").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn("Column UDT").SetCaption("UDTs").SetNumericFormat("#,###").TotalColumn()
                                                                 );

                                                                 this.DataTable.SetGroupHeader("Read-Repair", -1, true,
                                                                     this.DataTable.GetColumn("Read-Repair Chance (Max)").SetCaption("Chance (Max)").SetNumericFormat("##0%"),
                                                                     this.DataTable.GetColumn("Read-Repair DC Chance").SetCaption("DC Chance (Max)").SetNumericFormat("##0%")
                                                                     //this.DataTable.GetColumn("Read-Repair Policy (Majority)").SetCaption("Total")
                                                                 );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                    this.DataTable.GetColumn("GC Grace (Max)").SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat),
                                                                    this.DataTable.GetColumn("TTL (Max)").SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                 );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                    this.DataTable.GetColumn("OrderBy").SetNumericFormat("#,###").SetCaption("OrderBy (tables)")
                                                                    );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                   this.DataTable.GetColumn("Storage (MB)")
                                                                    .SetNumericFormat("###,###,###,###.0000")
                                                                    .TotalColumn()
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                   );

                                                                 //this.DataTable.GetColumn("DDL");

                                                                 workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                 workSheet.View.FreezePanes(3, 3);
                                                                 workSheet.ExcelRange(2,
                                                                                      this.DataTable.GetColumn("Name"),
                                                                                      this.DataTable.GetColumn("Storage (MB)"))
                                                                            .First().AutoFilter = true;

                                                                 workSheet.AutoFitColumn(workSheet.ExcelRange(this.DataTable.GetColumn("Name"),
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
