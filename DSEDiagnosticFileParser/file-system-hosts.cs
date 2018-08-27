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
    [JsonObject(MemberSerialization.OptIn)]
    public class file_system_hosts : DiagnosticFile
    {

        public file_system_hosts(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {

        }

        /// <summary>
        /// Setting this to true will result in new nodes being created (if not already defined) in the default DC from the host files.
        /// This should be set to false (default) for normal operations.
        /// </summary>
        public bool CreateNewNodeWhenNotFound { get; set; } = false;

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            uint nbrGenerated = 0;
            string line;

            foreach(var rawLine in fileLines)
            {
                line = rawLine.Trim();

                if (line == string.Empty) continue;

                var commentPos = line.IndexOf('#');

                if (commentPos == 0) continue;
                if(commentPos > 0)
                {
                    line = line.Substring(0, commentPos).Trim();

                    if (line == string.Empty) continue;
                }

                var hostList = line.Split(new char[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries).Select(h => h.Trim()).ToArray();

                if (hostList.Length <= 1) continue;

                INode node = null;

                if (this.CreateNewNodeWhenNotFound)
                {
                    if(NodeIdentifier.ValidNodeIdName(hostList[0]))
                    {
                        node = Cluster.TryGetAddNode(hostList[0], this.DefaultDataCenterName, this.DefaultClusterName);                          
                    }
                    else
                    {
                        continue;
                    }                    
                }
                else
                {
                    node = NodeIdentifier.ValidNodeIdName(hostList[0])
                            ? Cluster.TryGetNode(hostList[0], this.DefaultDataCenterName, this.DefaultClusterName)
                            : this.Node;

                    if (node == null)
                    {
                        if (hostList.Skip(1).Any(hn => NodeIdentifier.ValidNodeIdName(hn) && this.Node.Id.HostNameExists(hn)))
                        {
                            Logger.Instance.InfoFormat("FileMapper<{1}>\t{0}\t{2}\tAdded IP Address \"{3}\" from host file", node.Id, this.MapperId, this.ShortFilePath, hostList[0]);
                            this.Node.Id.SetIPAddressOrHostName(hostList[0]);
                            ++nbrGenerated;
                            node = this.Node;
                        }
                    }
                }                               

                if (node != null)
                {
                    ++this.NbrItemsParsed;

                    foreach (var hostName in hostList.Skip(1))
                    {
                        if (!NodeIdentifier.ValidNodeIdName(hostName)) continue;

                        if (Logger.Instance.IsDebugEnabled)
                        {
                            Logger.Instance.DebugFormat("FileMapper<{1}>\t{0}\t{2}\tAdded hostname \"{3}\" from host file", node.Id, this.MapperId, this.ShortFilePath, hostName);
                        }

                        node.Id.SetIPAddressOrHostName(hostName);
                        ++nbrGenerated;               
                    }
                }
             }

            this.Processed = true;
            return nbrGenerated;
        }
    }

    [JsonObject(MemberSerialization.OptIn)]
    public sealed class file_system_hosts_CreateNodes : file_system_hosts
    {
        public file_system_hosts_CreateNodes(CatagoryTypes catagory,
                                               IDirectoryPath diagnosticDirectory,
                                               IFilePath file,
                                               INode node,
                                               string defaultClusterName,
                                               string defaultDCName,
                                               Version targetDSEVersion)
           : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this.CreateNewNodeWhenNotFound = true;
        }
    }
}
