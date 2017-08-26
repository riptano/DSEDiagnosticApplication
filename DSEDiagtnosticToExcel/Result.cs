using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagtnosticToExcel
{
    public class Result
    {
        public IFilePath TargetExcelWorkbooks { get; }
        public IEnumerable<IFilePath> ExcelWorkbooks { get; }
    }
}
