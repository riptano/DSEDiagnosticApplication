using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;

namespace DSEDiagnosticLibrary
{

    public interface IResult
    {
        IPath Path { get; }
        Cluster Cluster { get; }
        IDataCenter DataCenter { get; }
        INode Node { get; }
        int NbrItems { get; }

        IEnumerable<IParsed> Results { get; }
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class EmptyResult : IResult
    {
        public EmptyResult(IPath path,
                            Cluster cluster,
                            IDataCenter dataCenter,
                            INode node)
        {
            this.Path = path;
            this.Cluster = cluster;
            this.DataCenter = dataCenter;
            this.Node = node;
        }

        [JsonConverter(typeof(IPathJsonConverter))]
        public IPath Path { get;}
        public Cluster Cluster { get; }
        public IDataCenter DataCenter { get; }
        public  INode Node { get; }
        [JsonIgnore]
        public int NbrItems { get; } = 0;
        [JsonIgnore]
        public IEnumerable<IParsed> Results { get; } = Enumerable.Empty<IParsed>();
        [JsonIgnore]
        public bool IsEmpty { get; } = true;
    }
}
