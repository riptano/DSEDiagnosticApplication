using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;
using OfficeOpenXml.Table;
using DT = DSEDiagnosticToDataTable;

namespace DSEDiagtnosticToExcel
{
    public sealed class PFReadLatencyExcel : PFExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.PFReadLatencyTable;

        public PFReadLatencyExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public PFReadLatencyExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        { }

        public override int FormatColumns()
        {
            this.DataTable.SetGroupHeader("Read", -1, true,
                this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMax)
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMax)
                    .SetCaption("Max(ms)")
                    .HideColumn(),
                this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMin)
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMin)
                    .SetCaption("Min(ms)")
                    .HideColumn(),
                this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadAvg)
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyAvg)
                    .SetCaption("Avg(ms)")
                    .HideColumn(),
                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadPercent)
                    .SetNumericFormat("##0.00%")
                    .SetCaption("Percent")
                    .SetComment("Total Percent Cells Read")
                    );

            this.DataTable.SetGroupHeader(string.Empty, -1, false,
                this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.KeysPercent)
                    .SetNumericFormat("##0.00%"),
                this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.StoragePercent)
                    .SetNumericFormat("##0.00%"));

            this.DataTable.SetGroupHeader("Informational", -2, true,
               this.DataTable.SetGroupHeader("Written", -1, true,
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetCaption("Factor"),
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WritePercent)
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        .SetComment("Total Percent Cells Written")
                        ));

            this.DataTable.SetGroupHeader("Influencer", -2, true,
               this.DataTable.SetGroupHeader("SSTable", -1, true,
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetCaption("Factor"),
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablePercent)
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
               this.DataTable.SetGroupHeader(string.Empty, -1, false,
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor),

                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor),

                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SecondaryIndexes)
                        .SetNumericFormat("##0"),
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.MaterializedViews)
                        .SetNumericFormat("##0")),

                this.DataTable.SetGroupHeader("Common Partition Keys", -1, true,
                   this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKey)
                        .SetCaption("Key"),
                    this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKeyFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetCaption("Factor")
                    ),
                this.DataTable.SetGroupHeader(string.Empty, -1, false,
                    this.DataTable.GetColumn(DT.TaggedDCDataTable.Columns.BaseTableFactor)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        )
                );
            return 3;
        }
    }
}
