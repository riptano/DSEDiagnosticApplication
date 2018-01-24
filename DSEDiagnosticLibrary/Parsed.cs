using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public enum SourceTypes
    {

        Unknown = 0,
        CassandraLog,
        CFStats,
        TPStats,
        Histogram,
        OpsCenterRepairSession,
        Yaml,
        EnvFile,
        CQL,
        JSON
    }

    public interface IParsed
    {
        SourceTypes Source { get; }
        IPath Path { get; }
        Cluster Cluster { get; }
        IDataCenter DataCenter { get; }
        INode Node { get; }
        int Items { get; }
        uint LineNbr { get; }
	}
}
