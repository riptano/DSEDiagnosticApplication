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
    public class file_nodemapping : DiagnosticFile
    {

        public file_nodemapping(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {

        }

        /*
            Datacenter: dc2 

            10.125.137.157	p-dc2-cassdb1-7.im.com 	7-dc2	
            10.125.139.45	p-dc2-cassdb1-13.im.com	13-dc2 
            10.125.139.44	p-dc2-cassdb1-12.im.com	12-dc2 
            10.125.139.47	p-dc2-cassdb1-14.im.com	14-dc2 
            10.125.136.174	p-dc2-cassdb1-2.im.com 	2-dc3
            10.125.139.46	p-dc2-cassdb1-15.im.com	15-dc2 
            10.125.139.41	p-dc2-cassdb1-8.im.com 	8-dc2
            10.125.139.43	p-dc2-cassdb1-11.im.com	11-dc2 
            10.125.139.42	p-dc2-cassdb1-10.im.com	10-dc2 
            10.125.136.197	p-dc2-cassdb1-4.im.com	4-dc2 
            
            Datacenter: sv1
            10.125.9.29	p-sv1-cassdb2-1.im.com 	1-sv1
            10.125.9.28	p-sv1-cassdb2-2.im.com 	2-sv1
         */

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            uint nbrGenerated = 0;
            string line;
            IDataCenter currentDC = null;
            bool initDC = false;

            foreach (var rawLine in fileLines)
            {
                line = rawLine.Trim();

                if (line == string.Empty || line.StartsWith("==")) continue;

                var commentPos = line.IndexOf('#');

                if (commentPos == 0) continue;
                if (commentPos > 0)
                {
                    line = line.Substring(0, commentPos).Trim();

                    if (line == string.Empty) continue;
                }

                if(line.StartsWith("datacenter:", StringComparison.OrdinalIgnoreCase))
                {
                    var dcNamePos = line.IndexOf(':');

                    if(dcNamePos > 0 && dcNamePos + 1 < line.Length)
                    {
                        var dcName = line.Substring(dcNamePos + 1).Trim();

                        if(dcName != string.Empty)
                        {
                            currentDC = Cluster.TryGetAddDataCenter(dcName, this.DefaultClusterName);
                            initDC = true;
                        }
                    }

                    continue;
                }
                else if(!initDC)
                {
                    if (currentDC == null && !string.IsNullOrEmpty(this.DefaultDataCenterName))
                    {
                        currentDC = Cluster.TryGetAddDataCenter(this.DefaultDataCenterName, this.DefaultClusterName);
                    }
                    initDC = true;
                }

                var hostList = line.Split(new char[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries).Select(h => h.Trim()).ToArray();

                if (hostList.Length <= 1) continue;

                INode node = null;

                if (NodeIdentifier.ValidNodeIdName(hostList[0]))
                {
                    node = Cluster.TryGetAddNode(hostList[0], currentDC);
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
}
