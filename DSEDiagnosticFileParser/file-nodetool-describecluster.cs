using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_nodetool_describecluster : DiagnosticFile
    {
        public file_nodetool_describecluster(CatagoryTypes catagory,
                                                IDirectoryPath diagnosticDirectory,
                                                IFilePath file,
                                                INode node,
                                                string defaultClusterName,
                                                string defaultDCName,
                                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {            
        }

        public file_nodetool_describecluster(IFilePath file,
                                                string defaultClusterName,
                                                string defaultDCName,
                                                Version targetDSEVersion = null)
            : base(CatagoryTypes.CommandOutputFile, file, defaultClusterName, defaultDCName, targetDSEVersion)
        {           
        }
        
        public override IResult GetResult()
        {
            return new EmptyResult(this.File, this.Node.Cluster, null, this.Node);
        }

        /*
          Cluster Information:
	        Name: dsealp
	        Snitch: org.apache.cassandra.locator.DynamicEndpointSnitch
	        Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	        Schema versions:
		        bc660eed-5c0d-386a-a309-2d99ca8259fe: [10.154.105.162, 10.154.105.163, 10.154.105.161]

            Cluster Information:
	        Name: NGCCluster
	        Snitch: org.apache.cassandra.locator.DynamicEndpointSnitch
	        Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	        Schema versions:
		        4301aff9-c9ed-3db6-abee-62adb390a7f7: [10.89.136.55]

		        a2daee40-7855-339d-8876-7cc385c58144: [10.43.104.40, 10.89.137.95, 10.89.137.94, 10.43.92.34, 10.89.137.112, 10.43.104.27, 10.43.106.27, 10.43.106.26, 10.43.106.25, 10.43.120.25, 10.43.104.24, 10.43.106.24, 10.43.104.28, 10.43.94.28, 10.89.137.110, 10.89.137.97, 10.89.137.96, 10.43.104.23, 10.43.106.23, 10.43.104.22, 10.43.106.22, 10.43.104.21, 10.106.180.42, 10.106.180.43, 10.106.180.41, 10.106.172.33, 10.106.172.58, 10.106.172.56, 10.106.172.57, 10.106.162.62, 10.106.172.62, 10.28.124.72, 10.28.132.74, 10.106.162.61, 10.106.172.61, 10.106.180.49, 10.106.154.54, 10.28.120.64, 10.106.154.55, 10.28.116.124, 10.43.122.75, 10.43.124.75, 10.89.136.57, 10.28.132.125, 10.43.122.74, 10.28.128.126, 10.28.132.126, 10.43.122.73, 10.43.122.72, 10.28.132.120, 10.89.136.61, 10.28.112.122, 10.89.136.63, 10.43.122.76, 10.43.124.76, 10.89.136.62, 10.43.118.67, 10.43.122.67, 10.89.136.49, 10.43.122.66, 10.89.136.48, 10.43.122.65, 10.43.120.71, 10.43.122.71, 10.28.120.113, 10.43.118.70, 10.43.120.70, 10.43.122.70, 10.43.118.69, 10.43.122.69, 10.43.118.68, 10.43.122.68, 10.89.144.54, 10.43.92.91, 10.28.124.109, 10.43.92.90, 10.43.92.89, 10.89.144.43, 10.89.144.42, 10.28.116.104, 10.89.136.45, 10.89.144.45, 10.89.136.44, 10.89.144.44, 10.43.94.93, 10.89.136.47, 10.43.94.92, 10.89.136.46, 10.89.144.46, 10.106.160.22, 10.28.112.96, 10.43.104.86, 10.106.154.20, 10.106.158.20, 10.106.162.20, 10.134.200.14, 10.135.216.15, 10.134.200.15, 10.135.216.14, 10.134.200.12, 10.135.216.13, 10.134.200.13, 10.135.216.12, 10.134.200.10, 10.135.216.11, 10.134.200.11, 10.135.216.10, 10.89.137.200, 10.134.200.18, 10.89.137.205, 10.134.200.19, 10.134.200.16, 10.134.200.17, 10.135.216.16, 10.89.137.206, 10.28.128.135, 10.89.137.199, 10.89.137.198, 10.132.92.36, 10.135.216.39, 10.132.92.37, 10.135.216.38, 10.132.92.38, 10.135.216.37, 10.132.92.39, 10.135.216.36, 10.135.216.35, 10.135.216.34, 10.43.106.141, 10.43.106.140, 10.135.216.43, 10.132.92.41, 10.135.216.42, 10.135.216.41, 10.135.216.40]

            Cluster Information:
	        Name: Production Cluster
            Snitch: org.apache.cassandra.locator.DynamicEndpointSnitch
            Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
            Schema versions:
              UNREACHABLE: 1176b7ac-8993-395d-85fd-41b89ef49fbb: [10.202.205.203]

         */

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            
            string line;
            string clusterName = null;
            string snitch = null;
            string partitioner = null;
            bool schemaVersionFnd = false;

            bool skipNextLine = false;
            
            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                if (skipNextLine)
                {
                    skipNextLine = false;
                    continue;
                }

                line = element.Trim();

                if (line == string.Empty) continue;

                if(schemaVersionFnd)
                {
                    var unreachable = false;
                    if(line.StartsWith("UNREACHABLE:", StringComparison.OrdinalIgnoreCase))
                    {
                        line = line.Substring(12).TrimStart();
                        unreachable = true;
                    }

                    var schemaVersionPos = line.IndexOf(':');
                    
                    if(schemaVersionPos > 0)
                    {                       
                        var schemaVersion = DSEDiagnosticLibrary.DSEInfo.VersionInfo.ParseSchema(line.Substring(0, schemaVersionPos).Trim());

                        if (!schemaVersion.HasValue) continue;

                        var nodes = line.Substring(schemaVersionPos + 1)
                                    .Trim('[', ']', ' ')
                                    .Split(new char[] { ',' })
                                    .Where(n => !string.IsNullOrEmpty(n));

                        foreach (var node in nodes)
                        {
                            var dseNode = NodeIdentifier.ValidNodeIdName(node)
                                            ? Cluster.TryGetAddNode(node, this.DefaultDataCenterName, this.Node.Cluster.Name)
                                            : this.Node;

                            if (dseNode.DSE.Versions.Schema.HasValue)
                            {
                                if(dseNode.DSE.Versions.Schema.Value != schemaVersion)
                                {                                    
                                    Logger.Instance.ErrorFormat("FileMapper<{2}>\t{1}\t{0}\tDifferent Node Schema Versions found. This Node's Schema Version is {3}, detected Schema Version is {4}. Setting Schema Version to null",
                                                                this.ShortFilePath,
                                                                this.Node.Id,
                                                                this.MapperId,
                                                                schemaVersion,
                                                                dseNode.DSE.Versions.Schema.Value);
                                    dseNode.DSE.Versions.Schema = null;
                                }
                                continue;
                            }

                            if (unreachable) dseNode.DSE.Statuses = DSEInfo.DSEStatuses.Unknown;
                            dseNode.DSE.Versions.Schema = schemaVersion;
                        }
                    }
                }

                if(line.StartsWith("name:", StringComparison.OrdinalIgnoreCase))
                {
                    clusterName = line.Substring(5).Trim();
                }
                else if (line.StartsWith("snitch:", StringComparison.OrdinalIgnoreCase))
                {
                    snitch = line.Substring(7).Trim();
                }
                else if (line.StartsWith("partitioner:", StringComparison.OrdinalIgnoreCase))
                {
                    partitioner = line.Substring(12).Trim();
                }
                else if (line.StartsWith("schema versions:", StringComparison.OrdinalIgnoreCase))
                {
                    schemaVersionFnd = true;
                }

                ++this.NbrItemsParsed;               
            }

            if(!string.IsNullOrEmpty(clusterName))
            {
                var cluster = Cluster.NameClusterAssociatewItem(this.Node, clusterName);
                if (cluster != null && cluster.DiagnosticDirectory == null) cluster.DiagnosticDirectory = this.DiagnosticDirectory;
            }
            if(!string.IsNullOrEmpty(snitch)
                    && string.IsNullOrEmpty(this.Node.DSE.EndpointSnitch))
            {
                this.Node.DSE.EndpointSnitch = snitch;
            }
            if(!string.IsNullOrEmpty(partitioner)
                && string.IsNullOrEmpty(this.Node.DSE.Partitioner))
            {
                this.Node.DSE.Partitioner = partitioner;
            }

            if (!this.Node.DSE.CaptureTimestamp.HasValue)
            {
                DateTimeOffset? captureDateTime = file_nodetool_info.DetermineNodeCaptureDateTime(this.Node, this.ShortFilePath, this.DiagnosticDirectory);

                if (captureDateTime.HasValue)
                {
                    this.Node.DSE.CaptureTimestamp = captureDateTime;

                    if (Logger.Instance.IsDebugEnabled)
                    {
                        Logger.Instance.InfoFormat("Node \"{0}\" using a Capture Date of {1}",
                                                    this.Node,
                                                    this.Node.DSE.CaptureTimestamp.HasValue
                                                        ? this.Node.DSE.CaptureTimestamp.Value.ToString(@"yyyy-MM-dd HH:mm:ss zzz")
                                                        : "<Unkown>");
                    }
                }
            }

            this.Processed = true;
            return (uint)this.NbrItemsParsed;
        }
    }
}
