using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagtnosticToExcel
{
    public static class LibrarySettings
    {
        public static Dictionary<string, DataTableToExcel.WorkSheetColAttrDefaults[]> WorkSheetDefaultAttrs = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<Dictionary<string, DataTableToExcel.WorkSheetColAttrDefaults[]>>(Properties.Settings.Default.WorkSheetDefaultAttrs);
        public static bool AppendToWorkSheet = Properties.Settings.Default.AppendToWorkSheet;
        public static string ExcelFileExtension = Properties.Settings.Default.ExcelFileExtension;
        public static bool ExcelPackageCache = Properties.Settings.Default.ExcelPackageCache;

        private static bool _ExcelSaveWorkSheet = Properties.Settings.Default.ExcelSaveWorkSheet;
        public static bool ExcelSaveWorkSheet
        {
            get
            {
                return ExcelPackageCache ? _ExcelSaveWorkSheet : true;
            }
            set
            {
                _ExcelSaveWorkSheet = value;
            }
        }
    }
}
