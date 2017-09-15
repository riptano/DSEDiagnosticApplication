using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticAnalytics
{
    public abstract class AnalyzeData : IAnalytics
    {
        #region constructors

        public AnalyzeData(Cluster cluster, CancellationToken cancellationToken)
        {
            this.CancellationToken = cancellationToken;
            this.Cluster = cluster;
        }

        #endregion

        #region IAnalytics

        public CancellationToken CancellationToken { get; }
        public Cluster Cluster { get; }
        public DataCenter DataCenter { get; }
        public Node Node { get; }
        public KeySpace KeySpace { get; }

        public abstract IEnumerable<IAggregatedStats> ComputeStats();
        #endregion
    }
}
