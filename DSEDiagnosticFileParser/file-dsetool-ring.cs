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
    public sealed class file_dsetool_ring : DiagnosticFile
    {
        public file_dsetool_ring(CatagoryTypes catagory,
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

            /*
             Address          DC                   Rack         Workload             Graph  Status  State    Load             Owns                 VNodes                                       Health [0,1]
            10.14.149.207    Ejby                 RAC1         Cassandra            no     Up      Normal   24.23 GB         ?                    256                                          0.90
            10.14.150.121    Ejby                 RAC1         Cassandra            no     Up      Normal   30.59 GB         ?                    256                                          0.90
            10.14.150.122    Ejby                 RAC1         Cassandra            no     Up      Normal   21.05 GB         ?                    256                                          0.90
            10.14.150.123    Ejby                 RAC1         Cassandra            no     Up      Normal   21.54 GB         ?                    256                                          0.90
            10.14.150.223    Ejby                 RAC1         Cassandra            no     Up      Normal   19.55 GB         ?                    256                                          0.90
            10.14.150.232    Brondby              RAC1         Cassandra            no     Up      Normal   27.14 GB         ?                    256                                          0.90
            10.14.150.233    Brondby              RAC1         Cassandra            no     Up      Normal   22.25 GB         ?                    256                                          0.90
            10.14.150.234    Brondby              RAC1         Cassandra            no     Up      Normal   23.59 GB         ?                    256                                          0.90
            10.14.150.235    Brondby              RAC1         Cassandra            no     Up      Normal   19.11 GB         ?                    256                                          0.90
            10.14.150.236    Brondby              RAC1         Cassandra            no     Up      Normal   31.46 GB         ?                    256                                          0.90
            Note: you must specify a keyspace to get ownership information.
            */
            /*
             Note: Ownership information does not include topology, please specify a keyspace.
            Address          DC           Rack         Workload         Status  State    Load             Owns                 VNodes
            10.1.36.100      DC1          RAC1         Cassandra        Up      Normal   432.26 GB        15.24%               256
            10.1.37.0        DC1_SPARK    RAC1         Cassandra        Up      Normal   822.63 GB        4.50%                64
            10.1.38.59       DC1          RAC1         Cassandra        Up      Normal   405.2 GB         14.20%               256
            10.1.39.216      DC1          RAC1         Cassandra        Up      Normal   502.72 GB        16.67%               256
            10.1.44.100      DC1          RAC1         Cassandra        Up      Normal   481.85 GB        14.23%               256
            10.1.44.20       DC1          RAC1         Cassandra        Up      Normal   438.59 GB        15.10%               256
            10.1.44.21       DC1          RAC1         Cassandra        Up      Normal   452.61 GB        14.94%               256
            10.1.46.110      DC1_SPARK    RAC1         Cassandra        Up      Normal   916.67 GB        5.12%                64
             */
            /*
              Note: Ownership information does not include topology, please specify a keyspace. 
                Address          DC           Rack         Workload         Status  State    Load             Owns                 Token                                       
                                                                                                                                   3074457345618258602                         
                10.20.70.20      Analytics    rack1        Analytics(JT)    Up      Normal   316.61 GB        16.67%               0                                           
                10.20.70.25      Analytics    rack1        Analytics(TT)    Up      Normal   316.98 GB        13.33%               5534023222112865484                         
                10.20.70.52      Cassandra    rack1        Cassandra        Up      Normal   315.01 GB        20.00%               -9223372036854775808                        
                10.20.70.82      Cassandra    rack1        Cassandra        Up      Normal   316.21 GB        33.33%               -3074457345618258603                        
                10.20.70.83      Cassandra    rack1        Cassandra        Up      Normal   315.75 GB        16.67%               3074457345618258602                         

            */
            /*
             Address          DC                   Rack         Workload             Graph  Status  State    Load             Owns                 VNodes                                       Health [0,1] 
            10.146.90.29     potomac              rack1        Analytics(SW)        no     Up      Normal   90.29 GiB        ?                    256                                          0.20         
            10.146.90.30     potomac              rack1        Analytics(SM)        no     Up      Normal   89.77 GiB        ?                    256                                          0.90         
            10.146.90.31     potomac              rack1        Analytics(SW)        no     Up      Normal   86.84 GiB        ?                    256                                          0.90         
            10.146.90.32     potomac              rack1        Analytics(SW)        no     Up      Normal   88.55 GiB        ?                    256                                          0.90         
            10.146.90.33     potomac              rack1        Analytics(SW)        no     Up      Normal   86 GiB           ?                    256                                          0.90         
            10.146.90.34     potomac              rack1        Analytics(SW)        no     Up      Normal   97.08 GiB        ?                    256                                          0.90         
            10.146.90.35     potomac              rack1        Analytics(SW)        no     Up      Normal   91.53 GiB        ?                    256                                          0.90         
            10.146.90.36     potomac              rack1        Analytics(SW)        no     Up      Normal   91.19 GiB        ?                    256                                          0.90         
            10.146.90.37     potomac              rack1        Analytics(SW)        no     Up      Normal   90.88 GiB        ?                    256                                          0.90         
            10.146.90.38     potomac              rack1        Analytics(SW)        no     Up      Normal   88.3 GiB         ?                    256                                          0.90         
            10.146.90.39     potomac              rack1        Analytics(SW)        no     Up      Normal   86.32 GiB        ?                    256                                          0.90         
            10.146.90.40     potomac              rack1        Analytics(SW)        no     Up      Normal   87.33 GiB        ?                    256                                          0.90         
            Note: you must specify a keyspace to get ownership information.

             */

            string line = null;
            string[] regExSplit = null;
            bool graphStatusCol = true;
            bool usesTokens = false;

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
                line = element.Trim();

                if(string.Empty == line
                    || line.StartsWith("Address")
                    || line.StartsWith("Note:"))
                {
                    usesTokens = line.EndsWith("Token");
                    continue;
                }

                if(usesTokens)
                {
                    if(line.Length <= 20
                            && line.All(i => char.IsNumber(i)))
                    {
                        continue;
                    }
                }

                //null,10.14.149.207,Ejby,RAC1,Cassandra,no,Up,Normal,24.23 GB,?,256
                regExSplit = this.RegExParser.Split(line);

                if(regExSplit.Length != 12)
                {
                    regExSplit = this.RegExParser.Split(line, 1);
                    graphStatusCol = false;

                    if (regExSplit.Length != 11)
                    {
                        Logger.Instance.ErrorFormat("FileMapper<{2}>\t<NoNodeId>\t{0}\tInvalid Line \"{1}\" found in DSETool Ring File.",
                                                    this.File,
                                                    line,
                                                    this.MapperId);
                        ++this.NbrErrors;
                        regExSplit = null;
                    }
                }

                if(regExSplit != null)
                {
                    var node = Cluster.TryGetAddNode(regExSplit[1], regExSplit[2], this.DefaultClusterName);

                    if (node != null)
                    {
                        DSEInfo.InstanceTypes type;
                        uint vNodes;

                        if (string.IsNullOrEmpty(node.DSE.Rack))
                        {
                            node.DSE.Rack = regExSplit[3];
                        }

                        if (Enum.TryParse<DSEInfo.InstanceTypes>(regExSplit[4], true, out type))
                        {
                            node.DSE.InstanceType |= type;
                        }
                        else
                        {
                            bool updated = false;

                            if (regExSplit[4][regExSplit[4].Length - 1] == ')')
                            {
                                var newName = regExSplit[4].Replace('(', '_').Substring(0, regExSplit[4].Length - 1);

                                if (updated = Enum.TryParse<DSEInfo.InstanceTypes>(newName, true, out type))
                                {
                                    node.DSE.InstanceType |= type;
                                }
                            }

                            if(!updated)
                            {
                                {
                                    var indexPos = regExSplit[4].IndexOf('(');
                                    if (indexPos < 0)
                                    {
                                        var instanceType = regExSplit[4].Substring(0, indexPos);
                                        var subType = regExSplit[4].Substring(indexPos + 1, regExSplit[4].Length - indexPos - 2);

                                        if (updated = Enum.TryParse<DSEInfo.InstanceTypes>(instanceType, true, out type))
                                        {
                                            node.DSE.InstanceType |= type;
                                        }

                                        if (!string.IsNullOrEmpty(subType))
                                        {
                                            if (updated = Enum.TryParse<DSEInfo.InstanceTypes>(subType, true, out type))
                                            {
                                                node.DSE.InstanceType |= type;
                                            }
                                        }
                                    }
                                }
                                if (!updated)
                                {
                                    Logger.Instance.WarnFormat("FileMapper<{2}>\t<NoNodeId>\t{0}\tInvalid DSE Instance Type of \"{1}\" found in DSETool Ring File. Type Ignored",
                                                                this.File,
                                                                regExSplit[4],
                                                                this.MapperId);
                                    ++this.NbrWarnings;
                                }
                            }
                        }

                        var offset = graphStatusCol ? 0 : -1;

                        if(graphStatusCol && regExSplit[5].ToLower() == "yes")
                        {
                            node.DSE.InstanceType |= DSEInfo.InstanceTypes.Graph;
                        }
                        
                        if (regExSplit[6 + offset].ToLower() == "up")
                        {
                            node.DSE.Statuses = DSEInfo.DSEStatuses.Up;
                        }

                        node.DSE.StorageUsed = new UnitOfMeasure(regExSplit[8 + offset]);

                        if (regExSplit[9 + offset][0] != '?')
                        {
                            node.DSE.StorageUtilization = new UnitOfMeasure(regExSplit[9 + offset], UnitOfMeasure.Types.Utilization | UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.Percent);
                        }

                        if (!usesTokens && uint.TryParse(regExSplit[10 + offset], out vNodes))
                        {
                            node.DSE.NbrTokens = vNodes;
                            node.DSE.VNodesEnabled = vNodes > 1;
                        }
                        else if(!string.IsNullOrEmpty(regExSplit[10 + offset]))
                        {
                            node.DSE.VNodesEnabled = false;
                        }

                        if(graphStatusCol && regExSplit[11][0] != '?' && regExSplit[11][0] != ' ')
                        {
                            node.DSE.HealthRating = regExSplit[11];
                        }

                        ++nbrGenerated;
                    }
                }

            }

            this.Processed = true;
            return nbrGenerated;
        }
    }
}
