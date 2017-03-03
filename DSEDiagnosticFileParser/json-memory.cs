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
                                    INode node)
			: base(catagory, diagnosticDirectory, file, node)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {
            jObject.TryGetValue("available").NullSafeSet<int>(c => this.Node.Machine.Memory.Available = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("cache").NullSafeSet<int>(c => this.Node.Machine.Memory.Cache = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("cached").NullSafeSet<int>(c => this.Node.Machine.Memory.Cache = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("buffers").NullSafeSet<int>(c => this.Node.Machine.Memory.Buffers = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("shared").NullSafeSet<int>(c => this.Node.Machine.Memory.Shared = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("free").NullSafeSet<int>(c => this.Node.Machine.Memory.Free = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));
            jObject.TryGetValue("used").NullSafeSet<int>(c => this.Node.Machine.Memory.Used = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.MiB));

            return 1;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
