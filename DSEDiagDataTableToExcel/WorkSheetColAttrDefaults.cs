using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataTableToExcel
{
    public struct WorkSheetColAttrDefaults
    {
        public WorkSheetColAttrDefaults(string column, params string[] attrs)
        {
            this.Column = column;
            this.Attrs = attrs;
        }

        public string Column;
        public string[] Attrs;
    }

}
