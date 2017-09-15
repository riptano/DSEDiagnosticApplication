using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticAnalytics
{
    public interface IAnalytics
    {
        CancellationToken CancellationToken { get; }
        DSEDiagnosticLibrary.Cluster Cluster { get; }
        DSEDiagnosticLibrary.DataCenter DataCenter { get; }
        DSEDiagnosticLibrary.Node Node { get; }
        DSEDiagnosticLibrary.KeySpace KeySpace { get; }

        IEnumerable<IAggregatedStats> ComputeStats();
    }
}
