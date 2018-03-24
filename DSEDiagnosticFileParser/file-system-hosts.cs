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
    public sealed class file_system_hosts : DiagnosticFile
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
                
                var node = hostList[0] == "127.0.0.1"
                                || hostList[0] == "0:0:0:0:0:0:0:1"
                                || hostList[0] == "::1"
                                || hostList[0] == "fe00::0"
                            ? this.Node
                            : Cluster.TryGetNode(hostList[0], this.DefaultDataCenterName, this.DefaultClusterName);
                
                if (node != null)
                {
                    ++this.NbrItemsParsed;

                    foreach (var hostName in hostList.Skip(1))
                    {
                        if (hostName.ToLower() == "localhost") continue;

                        node.Id.SetIPAddressOrHostName(hostName);
                        ++nbrGenerated;               
                    }
                }
             }

            this.Processed = true;
            return nbrGenerated;
        }
    }
}
