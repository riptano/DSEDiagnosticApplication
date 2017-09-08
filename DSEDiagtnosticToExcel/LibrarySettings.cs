﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagtnosticToExcel
{
    public static class LibrarySettings
    {
        public static Dictionary<string, DataTableToExcel.WorkSheetColAttrDefaults> WorkSheetDefaultAttrs = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<Dictionary<string, DataTableToExcel.WorkSheetColAttrDefaults>>(Properties.Settings.Default.WorkSheetDefaultAttrs);

    }
}