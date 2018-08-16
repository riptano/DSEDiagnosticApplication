using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_nodetool_version : DiagnosticFile
    {
        public file_nodetool_version(CatagoryTypes catagory,
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
            string line;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (!string.IsNullOrEmpty(line))
                {
                    if (line.StartsWith("releaseversion:", StringComparison.OrdinalIgnoreCase)
                            && this.Node.DSE.Versions.Cassandra == null)
                    {
                        this.Node.DSE.Versions.Cassandra = DSEInfo.VersionInfo.Parse(line.Substring(15));
                        ++this.NbrItemsParsed;
                    }
                }

            }

            this.Processed = true;
            return 0;
        }
    }
}
