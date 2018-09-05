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
         */
        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            
            string line;
            string clusterName = null;
            string snitch = null;
            string partitioner = null;

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
