using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticToDataTable
{
    public static class ColumnNames
    {
        public const string SessionId = "Session Id";
        public const string DataCenter = "Data Center";
        public const string NodeIPAddress = "Node IPAddress";
        public const string KeySpace = "Keyspace Name";
        public const string Table = "Table Name";
        public const string ReconciliationRef = "Reconciliation Reference";
        public const string Source = "Source";
        public const string LogLocalTimeStamp = "Log Local Timestamp";
        public const string LogLocalTZOffset = "Log Time Zone Offset";
        public const string UTCTimeStamp = "UTC Timestamp";
    }

    public static class TableNames
    {
        public const string Namespace = "DSEDiagnostic";
        public const string Config = "DSEConfiguration";
        public const string NodeConfigChanges = "DSENodeConfigChanges";
        public const string CQLDLL = "CQLDDL";
        public const string Keyspaces = "Keyspaces";
        public const string Machine = "Server";
        public const string Node = "Node";
        public const string TokenRanges = "TokenRanges";
        public const string CFStats = "CFStats";
        public const string NodeStats = "NodeStats";
        public const string LogInfo = "LogInformation";
        public const string MultiInstanceInfo = "MultiInstanceNodes";
        public const string LogAggregation = "LogAggregation";
        public const string CommonPartitionKey = "CommonPartitionKey";
    }
}
