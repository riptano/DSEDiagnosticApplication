using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalytics
{
    public sealed class AggregateGroups
    {
        private AggregateGroups() { }

        public AggregateGroups(string analyticsGroup, string aggregateGroupFunctionName)
        {
            this.AnalyticsGroup = analyticsGroup;
            this.AggregateGroupFunctionName = aggregateGroupFunctionName;
        }

        public string AnalyticsGroup { get; }

        private string _aggregateGroupFunctionName;
        public string AggregateGroupFunctionName
        {
            get { return this._aggregateGroupFunctionName; }
            set
            {
                if(this._aggregateGroupFunctionName != value)
                {
                    if(string.IsNullOrEmpty(value))
                    {
                        this.AggregateGroupFunction = null;
                        this._aggregateGroupFunctionName = null;
                    }
                    else
                    {
                        this.AggregateGroupFunction = Delegate.CreateDelegate(typeof(CassandraLogEventAggregate.AggregateGroupFunc),
                                                                                typeof(CassandraLogEventAggregate),
                                                                                value,
                                                                                true) as CassandraLogEventAggregate.AggregateGroupFunc;
                        if(this.AggregateGroupFunction == null)
                        {
                            throw new System.InvalidOperationException(string.Format("AnalyticsGroup \"{0}\" did not have any associated function named \"{1}\" defined in CassandraLogEventAggregate analytics assembly.",
                                                                                        this.AnalyticsGroup,
                                                                                        value));
                        }
                        this._aggregateGroupFunctionName = value;
                    }
                }
            }
        }

        public CassandraLogEventAggregate.AggregateGroupFunc AggregateGroupFunction
        {
            get;
            private set;
        }
    }
}
