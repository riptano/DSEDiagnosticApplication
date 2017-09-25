using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticToDataTable
{
    public abstract class DataTableLoad : IDataTable
    {
        public DataTableLoad(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource, Guid? sessionId = null)
        {
            this.Cluster = cluster;
            this.Table = this.CreateInitializationTable();

            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
            this.SessionId = sessionId;
        }

        public Guid? SessionId { get; }
        public DSEDiagnosticLibrary.Cluster Cluster { get; }
        public DataTable Table { get; }
        public CancellationToken CancellationToken { get; }

        public override string ToString()
        {
            return string.Format("{0}<Rows={1}>", this.Table?.TableName ?? "null", this.Table?.Rows.Count ?? 0);
        }

        abstract public DataTable CreateInitializationTable();

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        abstract public DataTable LoadTable();

        }
}
