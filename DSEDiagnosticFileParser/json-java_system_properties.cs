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
                                            INode node,
                                            string defaultClusterName,
                                            string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {
            jObject.TryGetValue("java.vendor").NullSafeSet<string>(c => this.Node.Machine.Java.Vendor = c);
            jObject.TryGetValue("sun.arch.data.model").NullSafeSet<int>(c => this.Node.Machine.Java.Model = c);
            jObject.TryGetValue("java.runtime.name").NullSafeSet<string>(c => this.Node.Machine.Java.RuntimeName = c);
            jObject.TryGetValue("java.runtime.version").NullSafeSet<string>(c => this.Node.Machine.Java.Version = c);
            jObject.TryGetValue("user.timezone").NullSafeSet<string>(c => this.Node.Machine.TimeZone = Common.TimeZones.Find(c));
            jObject.TryGetValue("dse.system_cpu_cores").EmptySafeSet<uint>(this.Node.Machine.CPU.Cores, c => this.Node.Machine.CPU.Cores = c);

            return 1;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
