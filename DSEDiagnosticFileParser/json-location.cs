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
    internal class json_location : ProcessJsonFile
    {
        public json_location(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName)
			: base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {

            jObject.TryGetValue("dse").NullSafeSet<string>(c => this.Node.DSE.Locations.DSEYamlFile = c);
            jObject.TryGetValue("cassandra").NullSafeSet<string>(c => this.Node.DSE.Locations.CassandraYamlFile = c);

            this.NbrItemsParsed = 2;
            this.Processed = true;
            return 0;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
