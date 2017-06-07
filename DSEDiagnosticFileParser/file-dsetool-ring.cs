using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
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
            throw new NotImplementedException();
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

            string line = null;
            string[] regExSplit = null;
            bool graphStatusCol = true;

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
                line = element.Trim();

                if(string.IsNullOrEmpty(line)
                    || line.StartsWith("Address")
                    || line.StartsWith("Note:"))
                {
                    continue;
                }

                //null,10.14.149.207,Ejby,RAC1,Cassandra,no,Up,Normal,24.23 GB,?,256
                regExSplit = this.RegExParser.Split(line);

                if(regExSplit.Length != 12)
                {
                    regExSplit = this.RegExParser.Split(line, 1);
                    graphStatusCol = false;

                    if (regExSplit.Length != 11)
                    {
                        Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tInvalid Line \"{1}\" found in DSETool Ring File.",
                                                    this.File,
                                                    line);
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

                        if (Enum.TryParse<DSEInfo.InstanceTypes>(regExSplit[4], out type))
                        {
                            node.DSE.InstanceType |= type;
                        }
                        else
                        {
                            Logger.Instance.WarnFormat("<NoNodeId>\t{0}\tInvalid DSE Instance Type of \"{1}\" found in DSETool Ring File. Type Ignored",
                                                            this.File,
                                                            regExSplit[4]);
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

                        if (uint.TryParse(regExSplit[10 + offset], out vNodes))
                        {
                            node.DSE.NbrTokens = vNodes;
                            node.DSE.VNodesEnabled = vNodes > 1;
                        }
                        else if(!string.IsNullOrEmpty(regExSplit[10 + offset]))
                        {
                            node.DSE.VNodesEnabled = false;
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
