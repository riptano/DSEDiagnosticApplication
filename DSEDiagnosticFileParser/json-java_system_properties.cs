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
    internal sealed class json_java_system_properties : ProcessJsonFile
    {
        public json_java_system_properties(CatagoryTypes catagory,
                                            IDirectoryPath diagnosticDirectory,
                                            IFilePath file,
                                            INode node)
            : base(catagory, diagnosticDirectory, file, node)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {
            jObject.TryGetValue("java.vendor").NullSafeSet<string>(c => this.Node.Machine.Java.Vendor = c);
            jObject.TryGetValue("sun.arch.data.model").NullSafeSet<int>(c => this.Node.Machine.Java.Model = c);
            jObject.TryGetValue("java.runtime.name").NullSafeSet<string>(c => this.Node.Machine.Java.RuntimeName = c);
            jObject.TryGetValue("java.runtime.version").NullSafeSet<string>(c => this.Node.Machine.Java.Version = new Version(c));
            jObject.TryGetValue("user.timezone").NullSafeSet<string>(c => this.Node.Machine.TimeZone = Common.TimeZones.Find(c));
            jObject.TryGetValue("dse.system_cpu_cores").NullSafeSet<int>(c => this.Node.Machine.CPU.Cores = (uint) c);

            return 1;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
