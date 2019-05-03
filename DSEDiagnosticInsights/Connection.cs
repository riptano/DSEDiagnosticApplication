using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticInsights
{
    public sealed class Connection : IDisposable
    {
       
        private Connection() { }

        private Connection(Uri esNode, DateTimeOffsetRange analysisPeriod, bool forceDebugging = false)
        {
            if (analysisPeriod == null || analysisPeriod.IsEmpty())
                throw new ArgumentException("analysis period is null or empty. It is required");

            this.AnalysisPeriod = analysisPeriod;
            this.ESConnection = new DSEDiagnosticInsightsES.ESConnection(esNode);
            this.ESClient = this.ESConnection.Create(forceDebugging);
        }

        public Connection(Uri esNode,
                            DateTimeOffsetRange analysisPeriod,
                            Guid insightClusterId,
                            bool createCluster = true,
                            bool forceDebugging = false)
            : this(esNode, analysisPeriod, forceDebugging)
        {
            this.InsightClusterId = insightClusterId;

            var clusterInfo = this.DetermineClusterInfo();
            this.ClusterName = clusterInfo.Item2.cluster_name;

            this.Nodes = clusterInfo.Item1.nodes;
            this.Keyspaces = clusterInfo.Item1.keyspaces;

            if (createCluster) this.Cluster = this.TryCreateCluster();
        }

        public Connection(Uri esNode,
                            DateTimeOffsetRange analysisPeriod,
                            string clusterName,
                            bool createCluster = true,
                            bool forceDebugging = false)
            : this(esNode, analysisPeriod, forceDebugging)
        {
            this.ClusterName = clusterName;

            var clusterInfo = this.DetermineClusterInfo();
            this.InsightClusterId = clusterInfo.Item2.clusterId;

            this.Nodes = clusterInfo.Item1.nodes;
            this.Keyspaces = clusterInfo.Item1.keyspaces;
            
            if (createCluster) this.Cluster = this.TryCreateCluster();
        }

        public DSEDiagnosticInsightsES.ESConnection ESConnection { get; }
        public Nest.IElasticClient ESClient { get; }

        public DateTimeOffsetRange AnalysisPeriod { get; }
        public Guid InsightClusterId { get; }
        //public Guid ClusterId { get; set; }
        public string ClusterName { get; }
        public IEnumerable<Node> Nodes { get; }
        public IEnumerable<Keyspace> Keyspaces { get;}

        public DSEDiagnosticLibrary.Cluster Cluster { get; }

        #region IDisposable Support
        public bool Disposed { get; private set; } = false; // To detect redundant calls

        void Dispose(bool disposing)
        {
            if (!this.Disposed)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).                        
                    this.ESConnection?.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                this.Disposed = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~Connection() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion


        private Tuple<DseCluster,DseNode> DetermineClusterInfo()
        {
            Nest.ISearchResponse<DseNode> nodeInfo = null;
            Nest.ISearchResponse<DseCluster> clusterInfo = null;
            bool internalThrow = false;

            try
            {
                nodeInfo = string.IsNullOrEmpty(this.ClusterName)
                                ? DseNode.BuilCurrentQuery(this.ESClient, this.InsightClusterId, this.AnalysisPeriod.Max)
                                : DseNode.BuilCurrentQuery(this.ESClient, this.ClusterName, this.AnalysisPeriod.Max);

                if (nodeInfo == null || nodeInfo.Documents == null || nodeInfo.Documents.IsEmpty())
                {
                    internalThrow = true;
                    if (nodeInfo == null || (this.InsightClusterId == Guid.Empty && string.IsNullOrEmpty(this.ClusterName)))
                        throw new ArgumentNullException("DSE Insight's Cluster Id and Cluster Name are both missing (null) or query failed. Required at least one.");

                    throw new KeyNotFoundException(string.Format("DSE Insights \"{0}\" ({1}) was not found.",
                                                                   string.IsNullOrEmpty(this.ClusterName) ? "Insight's Cluster Id" : "Cluster Name",
                                                                   string.IsNullOrEmpty(this.ClusterName) ? this.InsightClusterId.ToString() : this.ClusterName));
                }

                clusterInfo = DseCluster.BuilCurrentQuery(this.ESClient, this.InsightClusterId, this.AnalysisPeriod.Max);

                if (clusterInfo == null || clusterInfo.Documents == null || clusterInfo.Documents.IsEmpty())
                {
                    internalThrow = true;
                    throw new KeyNotFoundException(string.Format("DSE Insights Cluster \"{0}\" ({1}) was not found but DseNode Info was found.",
                                                                   this.InsightClusterId,
                                                                   this.ClusterName));
                }
            }
            catch (System.Exception ex)
            {
                if (internalThrow) throw;

                throw new InvalidOperationException(string.Format("DSE Insights \"{0}\" ({1}) encountered an error during the query.",
                                                                  string.IsNullOrEmpty(this.ClusterName) ? "Insight's Cluster Id" : "Cluster Name",
                                                                  string.IsNullOrEmpty(this.ClusterName) ? this.InsightClusterId.ToString() : this.ClusterName),
                                                        ex);
            }
            
            return new Tuple<DseCluster, DseNode>(clusterInfo.Documents.First(), nodeInfo.Documents.First());
        }

        private DSEDiagnosticLibrary.Cluster TryCreateCluster()
        {
            return DSEDiagnosticLibrary.Cluster.TryGetAddCluster(this.ClusterName, insightClusterId: this.InsightClusterId);
        }

    }
}
