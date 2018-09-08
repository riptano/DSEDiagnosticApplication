using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
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
            return new EmptyResult(this.File, this.Node?.Cluster, this.Node?.DataCenter, this.Node);
        }

        public override uint ProcessJSON(JObject jObject)
        {
            var values = jObject.TryGetValues();
            uint nbrGenerated = 0;

            if (values == null)
            {
                this.Processed = true;
                return nbrGenerated;
            }

            JObject nodeInfo;
            INode node;
            string dcName;
            IDataCenter currentDC;

            foreach (var keyValuePair in jObject.TryGetValues())
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                nodeInfo = keyValuePair.Value;
                dcName = nodeInfo.TryGetValue("dc")?.Value<string>() ?? this.DefaultDataCenterName;

                if (string.IsNullOrEmpty(dcName))
                {
                    if (!string.IsNullOrEmpty(keyValuePair.Key))
                    {
                        Logger.Instance.WarnFormat("FileMapper<{1}>\t<NoNodeId>\t{0}\tNode \"{2}\" was defined in OpsCenter node_info.json file but it had no DataCenter defined  Node Ignored",
                                                            this.ShortFilePath,
                                                            this.MapperId,
                                                            keyValuePair.Key);
                        this.NbrWarnings++;
                    }
                    continue;
                }
                else
                {
                    if(RingFileRead)
                    {
                        currentDC = Cluster.TryGetDataCenter(dcName, this.DefaultClusterName);

                        if(currentDC == null)
                        {
                            currentDC = Cluster.TryGetAddDataCenter(dcName + Properties.Settings.Default.UnknownDCSuffix, this.DefaultClusterName);
                            Logger.Instance.WarnFormat("FileMapper<{1}>\t<NoNodeId>\t{0}\tDataCenter \"{2}\" was not detected in ring file but defined in OpsCenter node_info.json file. Assuming a valid Data Center... ",
                                                            this.ShortFilePath,
                                                            this.MapperId,
                                                            dcName);
                            this.NbrWarnings++;
                        }
                    }
                    else
                    {
                        currentDC = Cluster.TryGetAddDataCenter(dcName, this.DefaultClusterName);
                    }
                }               

                if (RingFileRead)
                {
                    node = currentDC.TryGetNode(keyValuePair.Key);

                    if (node == null || node.DataCenter == null)
                    {                        
                        if(node == null)
                        {
                            node = Cluster.TryGetAddNode(keyValuePair.Key, currentDC);
                        }
                        else if(node.DataCenter == null)
                        {                            
                            Cluster.AssociateDataCenterToNode(currentDC.Name, node);
                        }

                        Logger.Instance.WarnFormat("FileMapper<{1}>\t{2}\t{0}\tNode \"{3}\" in DataCenter \"{4}\" was not detected in ring file but defined in OpsCenter node_info.json file. Assuming a valid Node... ",
                                                            this.ShortFilePath,
                                                            this.MapperId,
                                                            node,
                                                            keyValuePair.Key,
                                                            dcName);
                        this.NbrWarnings++;
                        node.DSE.Statuses = DSEInfo.DSEStatuses.Unknown;
                    }
                }
                else
                {
                    node = Cluster.TryGetAddNode(keyValuePair.Key, currentDC);
                }
                
                {
                    var jasonDSEVersions = nodeInfo.TryGetValue("node_version");

                    if (jasonDSEVersions != null)
                    {
                        jasonDSEVersions.TryGetValue("dse").NullSafeSet<string>(v => node.DSE.Versions.DSE = new Version(v));
                        jasonDSEVersions.TryGetValue("cassandra").NullSafeSet<string>(v => node.DSE.Versions.Cassandra = new Version(v));
                        jasonDSEVersions.TryGetValue("search").NullSafeSet<string>(v => node.DSE.Versions.Search = new Version(v));
                        jasonDSEVersions.TryGetValue("spark").TryGetValue("version").NullSafeSet<string>(v => node.DSE.Versions.Analytics = new Version(v));

                        if (node.DSE.Versions.Search != null)
                        {
                            node.DSE.InstanceType |= DSEInfo.InstanceTypes.Search;
                        }
                        if (node.DSE.Versions.Analytics != null)
                        {
                            node.DSE.InstanceType |= DSEInfo.InstanceTypes.Analytics;
                        }
                        if (node.DSE.Versions.Analytics == null
                                && node.DSE.Versions.Search == null
                                && node.DSE.Versions.Cassandra != null)
                        {
                            node.DSE.InstanceType |= DSEInfo.InstanceTypes.Cassandra;
                        }

                        this.NbrItemsParsed += 4;
                    }
                }

                {
                    var jsonDeviceLocations = nodeInfo.TryGetValue("devices");

                    if (jsonDeviceLocations != null)
                    {
                        jsonDeviceLocations.TryGetValue("commitlog").NullSafeSet<string>(v => node.DSE.Devices.CommitLog = v);
                        jsonDeviceLocations.TryGetValue("saved_caches").NullSafeSet<string>(v => node.DSE.Devices.SavedCache = v);

                        var items = jsonDeviceLocations.TryGetValue("data");

                        if (items != null)
                        {
                            var itemValues = new List<string>();

                            foreach (var item in items)
                            {
                                itemValues.Add(item.Value<string>());
                            }
                            node.DSE.Devices.Data = itemValues.ToArray();
                        }

                        items = jsonDeviceLocations.TryGetValue("other");

                        if (items != null)
                        {
                            var itemValues = new List<string>();

                            foreach (var item in items)
                            {
                                itemValues.Add(item.Value<string>());
                            }
                            node.DSE.Devices.Others = itemValues.ToArray();
                        }
                    }
                }

                nodeInfo.TryGetValue("ec2").TryGetValue("instance-type").NullSafeSet<string>(v => node.Machine.CloudVMType = v);
                nodeInfo.TryGetValue("ec2").TryGetValue("placement").NullSafeSet<string>(v => node.Machine.Placement = v);
                nodeInfo.TryGetValue("num_procs").EmptySafeSet<uint>(node.Machine.CPU.Cores, v => node.Machine.CPU.Cores = v);
                nodeInfo.TryGetValue("vnodes").NullSafeSet<bool>(v => node.DSE.VNodesEnabled = v);
                nodeInfo.TryGetValue("os").EmptySafeSet(node.Machine.OS, v => node.Machine.OS = v);
                nodeInfo.TryGetValue("hostname").NullSafeSet<string>(v => node.Id.SetIPAddressOrHostName(v, false));

                this.NbrItemsParsed += 6;                

                if (string.IsNullOrEmpty(node.DSE.Rack))
                {
                    ((Node)node).DSE.Rack = nodeInfo.TryGetValue("rack")?.Value<string>();
                    this.NbrItemsParsed += 1;
                }

                ++this.NbrItemsParsed;
            }
            
            this.Processed = true;
            return nbrGenerated;
        }
    }
}
