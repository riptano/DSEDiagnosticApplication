using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class json_java_system_properties : ProcessJsonFile
    {
        public json_java_system_properties(CatagoryTypes catagory,
                                            IDirectoryPath diagnosticDirectory,
                                            IFilePath file,
                                            INode node,
                                            string defaultClusterName,
                                            string defaultDCName,
                                            Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {
            jObject.TryGetValue("java.vendor").NullSafeSet<string>(c => this.Node.Machine.Java.Vendor = c);
            jObject.TryGetValue("sun.arch.data.model").NullSafeSet<int>(c => this.Node.Machine.Java.Model = c, null);
            jObject.TryGetValue("java.runtime.name").NullSafeSet<string>(c => this.Node.Machine.Java.RuntimeName = c);
            jObject.TryGetValue("java.runtime.version").NullSafeSet<string>(c => this.Node.Machine.Java.Version = c);
            jObject.TryGetValue("user.timezone").NullSafeSet<string>(c => this.Node.Machine.TimeZoneName = c);
            jObject.TryGetValue("dse.system_cpu_cores").EmptySafeSet<uint>(this.Node.Machine.CPU.Cores, c => this.Node.Machine.CPU.Cores = c);
            jObject.TryGetValue("os.version").NullSafeSet<string>(c => this.Node.Machine.Kernel = c);

            this.Node.UpdateDSENodeToolDateRange();

            uint nLine = 0;

            foreach(var jItem in jObject)
            {
                try
                {
                    YamlConfigurationLine.AddConfiguration(this.File,
                                                            this.Node,
                                                            ++nLine,
                                                            jItem.Key,
                                                            jItem.Value.Value<string>(),
                                                            ConfigTypes.Java,
                                                            SourceTypes.JSON);
                }
                catch(System.Exception ex)
                {
                    Logger.Instance.WarnFormat("FileMapper<{2}>\t{0}\t{1}\tJSON Key \"{3}\" at Line {6} throw an exception of \"{4}\" ({5}). Key Ignored",
                                                this.Node.Id,
                                                this.ShortFilePath,
                                                this.MapperId,
                                                jItem.Key,
                                                ex.Message,
                                                ex.GetType().Name,
                                                nLine);
                    ++this.NbrWarnings;
                }
            }

            this.NbrItemsParsed = 6 + (int) nLine;




            this.Processed = true;
            return 0;
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }
    }
}
