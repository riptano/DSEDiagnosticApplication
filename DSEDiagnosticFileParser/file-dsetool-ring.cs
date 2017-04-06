using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Common;
using DSEDiagnosticLibrary;

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

            string line = null;
            string[] regExSplit = null;

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

                if(regExSplit.Length <= 10)
                {
                    Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tInvalid Line \"{1}\" found in DSETool Ring File.",
                                                this.File,
                                                line);
                }
                else
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
                        if(regExSplit[5].ToLower() == "yes")
                        {
                            node.DSE.InstanceType |= DSEInfo.InstanceTypes.Graph;
                        }
                        if (regExSplit[6].ToLower() == "up")
                        {
                            node.DSE.Statuses = DSEInfo.DSEStatuses.Up;
                        }

                        node.DSE.StorageUsed = new UnitOfMeasure(regExSplit[8]);

                        if (uint.TryParse(regExSplit[10], out vNodes))
                        {
                            node.DSE.NbrTokens = vNodes;
                            node.DSE.VNodesEnabled = vNodes > 1;
                        }
                        else if(!string.IsNullOrEmpty(regExSplit[10]))
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
