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
    public sealed class file_agent_version : DiagnosticFile
    {
        public file_agent_version(CatagoryTypes catagory, 
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
            var fileLine = this.File.ReadAllText();
            
            if (!string.IsNullOrEmpty(fileLine))
            {
                this.Node.DSE.Versions.OpsCenterAgent = new Version(fileLine);
                ++this.NbrItemsParsed;
            }

            this.Processed = true;
            return 0;
        }
    }
}
