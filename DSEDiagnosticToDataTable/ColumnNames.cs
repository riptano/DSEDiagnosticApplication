using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticToDataTable
{
    public static class ColumnNames
    {
        public const string DataCenter = "Data Center";
        public const string NodeIPAddress = "Node IPAddress";
        public const string KeySpace = "Keyspace Name";
    }

    public static class TableNames
    {
        public const string Namespace = "DSEDiagnostic";
        public const string Config = "DSEConfiguration";
        public const string CQLDLL = "CQLDDL";
        public const string Keyspaces = "Keyspaces";
        public const string Machine = "Server";
        public const string Node = "Node";
        public const string TokenRanges = "TokenRanges";        
    }
}
