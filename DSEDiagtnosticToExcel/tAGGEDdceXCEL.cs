﻿using System;
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
    public sealed class TaggedDCExcel : TaggedItemsExcel
    {
        public new const string DataTableName = DSEDiagnosticToDataTable.TableNames.TaggedDCTables;

        public TaggedDCExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public TaggedDCExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook)
        { }
    }
}