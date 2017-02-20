using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    internal sealed class json_node_info : ProcessJsonFile
    {
        private json_node_info(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node)
			: base(catagory, diagnosticDirectory, file, node)
		{
        }

        public override IEnumerable<T> GetItems<T>()
        {
            throw new NotImplementedException();
        }

        public override uint ProcessJSON(JObject jObject)
        {
            uint nbrItems = 0;
            JToken nodeInfo = null;

            foreach (var ipAdress in this.Node.Id.Addresses)
            {
                nodeInfo = jObject.TryGetValue(ipAdress.ToString());

                if (nodeInfo != null)
                    break;
            }

            if (nodeInfo != null)
            {
                {
                    var jasonDSEVersions = nodeInfo.TryGetValue("node_version");

                    if (jasonDSEVersions != null)
                    {
                        jasonDSEVersions.TryGetValue("dse").NullSafeSet<string>(v => this.Node.DSE.Versions.DSE = new Version(v));
                        jasonDSEVersions.TryGetValue("cassandra").NullSafeSet<string>(v => this.Node.DSE.Versions.Cassandra = new Version(v));
                        jasonDSEVersions.TryGetValue("search").NullSafeSet<string>(v => this.Node.DSE.Versions.Search = new Version(v));
                        jasonDSEVersions.TryGetValue("spark").TryGetValue("version").NullSafeSet<string>(v => this.Node.DSE.Versions.Analytics = new Version(v));
                    }
                }

                nodeInfo.TryGetValue("ec2").TryGetValue("instance-type").NullSafeSet<string>(v => v.ParseEnumString<DSEInfo.InstanceTypes>(ref this.Node.DSE.InstanceType));
                nodeInfo.TryGetValue("num_procs").NullSafeSet<uint>(v => this.Node.Machine.CPU.Cores = v);
                nodeInfo.TryGetValue("vnodes").NullSafeSet<uint>(v => this.Node.DSE.NbrVNodes = v);

            }

            return nbrItems;
        }
    }
}
