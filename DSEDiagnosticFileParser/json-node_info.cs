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
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class json_node_info : ProcessJsonFile
    {
        public json_node_info(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
			: base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
		{
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }

        public override uint ProcessJSON(JObject jObject)
        {
            JToken nodeInfo = null;
            uint nbrGenerated = 0;

            foreach (var ipAdress in this.Node.Id.Addresses)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
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

                        if(this.Node.DSE.Versions.Search != null)
                        {
                            this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Search;
                        }
                        if (this.Node.DSE.Versions.Analytics != null)
                        {
                            this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Analytics;
                        }
                        if (this.Node.DSE.Versions.Analytics == null
                                && this.Node.DSE.Versions.Search == null
                                && this.Node.DSE.Versions.Cassandra != null)
                        {
                            this.Node.DSE.InstanceType |= DSEInfo.InstanceTypes.Cassandra;
                        }

                        this.NbrItemsParsed += 4;
                    }
                }

                {
                    var jsonDeviceLocations = nodeInfo.TryGetValue("devices");

                    if(jsonDeviceLocations != null)
                    {
                        jsonDeviceLocations.TryGetValue("commitlog").NullSafeSet<string>(v => this.Node.DSE.Devices.CommitLog = v);
                        jsonDeviceLocations.TryGetValue("saved_caches").NullSafeSet<string>(v => this.Node.DSE.Devices.SavedCache = v);

                        var items = jsonDeviceLocations.TryGetValue("data");

                        if (items != null)
                        {
                            var itemValues = new List<string>();

                            foreach (var item in items)
                            {
                                itemValues.Add(item.Value<string>());
                            }
                            this.Node.DSE.Devices.Data = itemValues.ToArray();                            
                        }

                        items = jsonDeviceLocations.TryGetValue("other");

                        if (items != null)
                        {
                            var itemValues = new List<string>();

                            foreach (var item in items)
                            {
                                itemValues.Add(item.Value<string>());
                            }
                            this.Node.DSE.Devices.Others = itemValues.ToArray();                            
                        }
                    }
                }

                nodeInfo.TryGetValue("ec2").TryGetValue("instance-type").NullSafeSet<string>(v => this.Node.Machine.CloudVMType = v);
                nodeInfo.TryGetValue("ec2").TryGetValue("placement").NullSafeSet<string>(v => this.Node.Machine.Placement = v);
                nodeInfo.TryGetValue("num_procs").EmptySafeSet<uint>(this.Node.Machine.CPU.Cores, v => this.Node.Machine.CPU.Cores = v);
                nodeInfo.TryGetValue("vnodes").NullSafeSet<bool>(v => this.Node.DSE.VNodesEnabled = v);
                nodeInfo.TryGetValue("os").EmptySafeSet(this.Node.Machine.OS, v => this.Node.Machine.OS = v);
                this.NbrItemsParsed += 4;

                if (string.IsNullOrEmpty(this.Node.Id.HostName))
                {
                    nodeInfo.TryGetValue("hostname").NullSafeSet<string>(v => this.Node.Id.SetIPAddressOrHostName(v, false) );
                    this.NbrItemsParsed += 1;
                }

                if(this.Node.DataCenter == null)
                {
                    var dcName = nodeInfo.TryGetValue("dc")?.Value<string>();

                    if (!string.IsNullOrEmpty(dcName))
                    {
                        if (Cluster.AssociateDataCenterToNode(dcName, this.Node) != null)
                        {
                            ++nbrGenerated;
                        }                        
                    }
                }

                if (string.IsNullOrEmpty(this.Node.DSE.Rack))
                {
                    ((Node)this.Node).DSE.Rack = nodeInfo.TryGetValue("rack")?.Value<string>();
                    this.NbrItemsParsed += 1;
                }
            }

            this.Processed = true;
            return nbrGenerated;
        }
    }
}
