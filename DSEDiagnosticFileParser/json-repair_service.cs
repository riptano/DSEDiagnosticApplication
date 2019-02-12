using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public class json_repair_service : DiagnosticFile
    {
        public json_repair_service(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
			: base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
		{
        }

        public override uint ProcessFile()
        {
            var jsonFile = this.File.ReadAllText();
            var infoObject = JsonConvert.DeserializeAnonymousType(jsonFile, new { time_to_completion = 0L, status = string.Empty, parallel_tasks = 0m, all_tasks = new object[1][] } );

            if(infoObject.all_tasks != null)
            {
                this.Node.DSE.RepairServiceHasRan = infoObject.all_tasks.Any(c => this.Node.Id.Addresses.Any(i => i.ToString() == (string)((object[])c)[0]));
            }

            this.NbrItemsParsed = 1;
            this.Processed = true;
            return 0;
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }
    }
}
