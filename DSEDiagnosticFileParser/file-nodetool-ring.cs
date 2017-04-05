using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    public sealed class file_nodetool_ring : DiagnosticFile
    {
        public file_nodetool_ring(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
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
             Datacenter: Brondby
             ==========
             Address        Rack        Status State   Load            Owns                Token
                                                                              9203908348549599789
             10.14.150.236  RAC1        Up     Normal  31.46 GB        ?                   -9195822582950075098
             10.14.150.235  RAC1        Up     Normal  19.11 GB        ?                   -9195778974219174770
             10.14.150.234  RAC1        Up     Normal  23.59 GB        ?                   -9162347131910050116
             10.14.150.234  RAC1        Up     Normal  23.59 GB        ?                   -9147150508342489016
             10.14.150.236  RAC1        Up     Normal  31.46 GB        ?                   -9116835335818530346
             10.14.150.234  RAC1        Up     Normal  23.59 GB        ?                   9167187276438601770
             10.14.150.232  RAC1        Up     Normal  27.14 GB        ?                   9203908348549599789

            Datacenter: Ejby
            ==========
            Address        Rack        Status State   Load            Owns                Token
                                                                              9212160192835661072
            10.14.150.122  RAC1        Up     Normal  21.05 GB        ?                   -9205136167623241595
            10.14.150.223  RAC1        Up     Normal  19.55 GB        ?                   -9194029062598249143
            10.14.150.223  RAC1        Up     Normal  19.55 GB        ?                   -9186275386149691165
            10.14.150.223  RAC1        Up     Normal  19.55 GB        ?                   9158549389804271028
            10.14.150.223  RAC1        Up     Normal  19.55 GB        ?                   9212160192835661072

            Warning: "nodetool ring" is used to output all the tokens of a node.
            To view status related info of a node use "nodetool status" instead.


            Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless

            */

            string line = null;
            string[] regExSplit = null;
            IDataCenter currentDataCenter = null;
            string startToken = null;

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
                line = element.Trim();

                if (string.IsNullOrEmpty(line)
                    || line.StartsWith("Address")
                    || line.StartsWith("Warning:")
                    || line.StartsWith("Note:")
                    || line.StartsWith("To view")
                    || line.StartsWith("=="))
                {
                    continue;
                }

                //null,Brondby
                //null,9203908348549599789

                regExSplit = this.RegExParser.Split(line, 0); //DC

                if (regExSplit.Length == 3)
                {
                    currentDataCenter = Cluster.TryGetDataCenter(regExSplit[1], this.DefaultClusterName);                   
                    continue;
                }

                regExSplit = this.RegExParser.Split(line, 1); //Start Token
                if (regExSplit.Length == 3)
                {
                    startToken = regExSplit[1];
                    continue;
                }

                regExSplit = this.RegExParser.Split(line, 2); //File Line

                //null,10.14.150.223,RAC1,Up,Normal,19.55 GB,?,9212160192835661072
                if (regExSplit.Length <= 7)
                {
                    Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tInvalid Line \"{1}\" found in nodetool Ring File.",
                                                this.File,
                                                line);
                }
                else
                {
                    var node = Cluster.TryGetAddNode(regExSplit[1], currentDataCenter);

                    if (node != null)
                    {
                        ++nbrGenerated;

                        if (string.IsNullOrEmpty(node.DSE.Rack))
                        {
                            node.DSE.Rack = regExSplit[2];
                        }

                        if (regExSplit[3].ToLower() == "up")
                        {
                            node.DSE.Statuses = DSEInfo.DSEStatuses.Up;
                        }

                        try
                        {
                            node.DSE.AddTokenPair(startToken, regExSplit[7], regExSplit[5]);
                            startToken = regExSplit[7];
                        }
                        catch (System.ArgumentException ex)
                        {
                            Logger.Instance.Error(string.Format("<NoNodeId>\t{0}\tInvalid Token Range found for \"{1}\" in nodetool Ring File.",
                                                                this.File,
                                                                line),
                                                    ex);
                        }
                    }
                }

            }

            this.Processed = true;
            return nbrGenerated;
        }
    }
}
