using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;
using DT = DSEDiagnosticToDataTable;

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
                                                                                                        this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Name),
                                                                                                        null,
                                                                                                        this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Total),
                                                                                                        this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Tables),
                                                                                                        this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.DDL));

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

                                                                 this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Name)
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonKeySpaceNameActivite);
                                                                 this.DataTable.SetGroupHeader("Replication", -1, true,
                                                                     this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReplicationStrategy),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReplicationFactor).SetNumericFormat("#,###")
                                                                  );
                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Tables).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Views).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SecondaryIndexes).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SolrIndexes).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SASIndexes).SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.CustomIndexes).SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Functions).SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Triggers).SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Total).SetNumericFormat("#,##0").TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTotTbls),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Active).SetNumericFormat("#,##0").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonActiveTbls),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ActivePercent).SetNumericFormat("##0%")
                                                                        .TotalFormulaColumn(string.Format("={{{0}}}{{3}}/sum({{{1}}}{{3}}:{{{2}}}{{3}},{{{3}}}{{3}}:{{{4}}}{{3}})", 
                                                                                                            DT.KeyspaceDataTable.Columns.Active,
                                                                                                            DT.KeyspaceDataTable.Columns.Tables,
                                                                                                            DT.KeyspaceDataTable.Columns.SecondaryIndexes,
                                                                                                            DT.KeyspaceDataTable.Columns.SASIndexes,
                                                                                                            DT.KeyspaceDataTable.Columns.CustomIndexes))
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonActiveTblsPercent)
                                                                     );
                                                                 this.DataTable.SetGroupHeader("Compaction Strategy", -1, true,
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.STCS).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.LCS).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.DTCS).SetNumericFormat("#,###").TotalColumn()
                                                                         .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.TCS).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.TWCS).SetNumericFormat("#,###").TotalColumn(),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.OtherStrategies).SetNumericFormat("#,###").TotalColumn()
                                                                  );
                                                                 this.DataTable.SetGroupHeader("Columns", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnTotal).SetCaption("Total").SetNumericFormat("#,##0").TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTotTbls),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnCollections)
                                                                        .SetCaption("Collections").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnBlobs)
                                                                        .SetCaption("Blobs").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnStatic)
                                                                        .SetCaption("Statics").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnFrozen)
                                                                        .SetCaption("Frozen").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnTuple)
                                                                        .SetCaption("Tuples").SetNumericFormat("#,###").TotalColumn(),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ColumnUDT)
                                                                        .SetCaption("UDTs").SetNumericFormat("#,###").TotalColumn()
                                                                 );

                                                                 this.DataTable.SetGroupHeader("Read-Repair", -1, true,
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReadRepairChance)
                                                                        .SetCaption("Chance (Max)").SetNumericFormat("##0%"),
                                                                     this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReadRepairDCChance)
                                                                        .SetCaption("DC Chance (Max)").SetNumericFormat("##0%")
                                                                    //this.DataTable.GetColumn("DT.KeyspaceDataTable.Columns.ReadRepairPolicy).SetCaption("Total")
                                                                 );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.GCGrace)
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.TTL)
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                 );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.OrderBy)
                                                                        .SetNumericFormat("#,###").SetCaption("OrderBy (tables)").TotalColumn()
                                                                    );

                                                                 this.DataTable.SetGroupHeader("Compact", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.CompactStorage)
                                                                        .SetNumericFormat("#,###")
                                                                        .SetCaption("Storage")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDDLWarnUse)
                                                                    );

                                                                 this.DataTable.SetGroupHeader(" ", -1, true,
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Storage)
                                                                        .SetNumericFormat("###,###,###,###.0000")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.StorageUtilized)
                                                                        .SetNumericFormat("##0%")                                                                        
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReadPercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.WritePercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.PartitionKeys)
                                                                        .SetNumericFormat("###,###,###,##0")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.PartitionKeyPercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTables)
                                                                        .SetNumericFormat("###,###,###,##0")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTablePercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                   );

                                                                 //this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.DDL);

                                                                 workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                 workSheet.View.FreezePanes(3, 3);
                                                                 workSheet.ExcelRange(2,
                                                                                      this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Name),
                                                                                      this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTablePercent))
                                                                            .First().AutoFilter = true;

                                                                 workSheet.AutoFitColumn(workSheet.ExcelRange(this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Name),
                                                                                                                this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTablePercent)));
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
