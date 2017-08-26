using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagtnosticToExcel
{
    public static class Logger
    {
        public static readonly DataTableToExcel.Logger Instance = DataTableToExcel.Logger.Instance;
    }
}
