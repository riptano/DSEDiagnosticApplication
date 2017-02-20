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
    class json_repair_service : DiagnosticFile
    {
        public json_repair_service(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node)
			: base(catagory, diagnosticDirectory, file, node)
		{
        }

        readonly object RepairServiceDefinition = new { time_to_completion = 0L, status = string.Empty, parallel_tasks = 0, all_tasks = new object[1][] };

        public override uint ProcessFile()
        {
            uint nbrItems = 0;
            var jsonFile = this.File.ReadAllText();
            var infoObject = JsonConvert.DeserializeAnonymousType(jsonFile, RepairServiceDefinition);

            //foreach (DataRow dataRow in dtRingInfo.Rows)
            //{
            //    if (infoObject.all_tasks != null
            //            && infoObject.all_tasks.Any((c => (string)((object[])c)[0] == (string)dataRow["Node IPAddress"])))
            //    {
            //        dataRow["Repair Service Enabled"] = true;
            //    }
            //}

            return nbrItems;
        }

        public override IEnumerable<T> GetItems<T>()
        {
            throw new NotImplementedException();
        }
    }
}
