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

        public AnalyzeData(INode node, CancellationToken cancellationToken)
        {
            this.CancellationToken = cancellationToken;
            this.Cluster = node.Cluster;
            this.DataCenter = node.DataCenter;
            this.Node = node;
        }

        #endregion

        #region IAnalytics

        public CancellationToken CancellationToken { get; }
        public Cluster Cluster { get; }
        public IDataCenter DataCenter { get; }
        public INode Node { get; }
        public IKeyspace KeySpace { get; }

        public abstract IEnumerable<IAggregatedStats> ComputeStats();
        #endregion
    }
}
