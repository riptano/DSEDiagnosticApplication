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
    internal sealed class json_memory : ProcessJsonFile
    {
        public json_memory(CatagoryTypes catagory,
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
            jObject.TryGetValue("available").NullSafeSet<long>(c => this.Node.Machine.Memory.Available = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("cache").NullSafeSet<long>(c => this.Node.Machine.Memory.Cache = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("cached").NullSafeSet<long>(c => this.Node.Machine.Memory.Cache = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("buffers").NullSafeSet<long>(c => this.Node.Machine.Memory.Buffers = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("shared").NullSafeSet<long>(c => this.Node.Machine.Memory.Shared = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("free").NullSafeSet<long>(c => this.Node.Machine.Memory.Free = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("used").NullSafeSet<long>(c => this.Node.Machine.Memory.Used = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));

            this.NbrItemsParsed = 7;
            this.Processed = true;
            return 1;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
