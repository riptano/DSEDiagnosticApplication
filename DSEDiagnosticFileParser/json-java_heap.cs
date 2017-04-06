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
    internal sealed class json_java_heap : ProcessJsonFile
    {
        public json_java_heap(CatagoryTypes catagory,
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
            jObject.TryGetValue("NonHeapMemoryUsage").TryGetValue("committed").NullSafeSet<long>(c => this.Node.Machine.Java.NonHeapMemory.Committed = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("NonHeapMemoryUsage").TryGetValue("init").NullSafeSet<long>(c => this.Node.Machine.Java.NonHeapMemory.Initial = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("NonHeapMemoryUsage").TryGetValue("max").NullSafeSet<long>(c => this.Node.Machine.Java.NonHeapMemory.Maximum = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("NonHeapMemoryUsage").TryGetValue("used").NullSafeSet<long>(c => this.Node.Machine.Java.NonHeapMemory.Used = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));

            jObject.TryGetValue("HeapMemoryUsage").TryGetValue("committed").NullSafeSet<long>(c => this.Node.Machine.Java.HeapMemory.Committed = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("HeapMemoryUsage").TryGetValue("init").NullSafeSet<long>(c => this.Node.Machine.Java.HeapMemory.Initial = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("HeapMemoryUsage").TryGetValue("max").NullSafeSet<long>(c => this.Node.Machine.Java.HeapMemory.Maximum = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));
            jObject.TryGetValue("HeapMemoryUsage").TryGetValue("used").NullSafeSet<long>(c => this.Node.Machine.Java.HeapMemory.Used = UnitOfMeasure.Create(c, UnitOfMeasure.Types.Memory | UnitOfMeasure.Types.Byte));

            this.NbrItemsParsed = 8;
            this.Processed = true;
            return 0;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
