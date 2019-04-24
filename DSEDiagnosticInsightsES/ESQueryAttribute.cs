using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticInsightsES
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
    public class ESQueryAttribute : System.Attribute
    {

        public ESQueryAttribute(string insightName, bool createESIndex = true, string esIndexName = null)
        {
            this.ESIndexName = esIndexName ?? insightName;
            this.Name = insightName;
            if (createESIndex)
                this.ESIndex = Nest.Indices.Parse(this.ESIndexName);
        }
              
        public ESQueryAttribute(string insightName,                                    
                                    string queryFieldName,
                                    string queryTimestampFiledName = null,
                                    string esIndexName = null,
                                    string mappingVersion = null,
                                    bool createESIndex = true)
            : this(insightName, createESIndex, esIndexName)
        {           
            this.QueryFieldName = queryFieldName;
            this.QueryTimestampFieldName = queryTimestampFiledName;
            this.MappingVersion = mappingVersion;
        }

        public string ESIndexName { get; }
        public Nest.Indices ESIndex { get; }
        public string Name { get; }
        public string MappingVersion { get; }
        public string QueryFieldName { get; }
        public string QueryTimestampFieldName { get; }

        public Nest.Indices GetIndexForTimeFrame(DateTimeOffset timeframe)
        {
            var idxName = string.Format("{0}-{1:yyyy.MM.dd}", this.Name, timeframe.UtcDateTime);

            return Nest.Indices.Parse(idxName);
        }

    }
}
