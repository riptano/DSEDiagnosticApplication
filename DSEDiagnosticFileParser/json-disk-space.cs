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
    public sealed class json_disk_space : ProcessJsonFile
    {
        public json_disk_space(CatagoryTypes catagory,
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
            uint nbrGenerated = 0;

            {
                var free = jObject.TryGetValue("free");

                if (free != null)
                {
                    ++this.NbrItemsParsed;
                    var devices = free.ToObject<Dictionary<string, decimal>>();

                    this.Node.Machine.Devices.Free = devices.Select(i => new KeyValuePair<string, UnitOfMeasure>(i.Key, new UnitOfMeasure(i.Value, UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.MiB)));
                    this.NbrItemsParsed += devices.Count;
                    nbrGenerated += (uint)devices.Count;
                }
            }

            {
                var used = jObject.TryGetValue("used");

                if (used != null)
                {
                    ++this.NbrItemsParsed;

                    var devices = used.ToObject<Dictionary<string, decimal>>();

                    this.Node.Machine.Devices.Used = devices.Select(i => new KeyValuePair<string, UnitOfMeasure>(i.Key, new UnitOfMeasure(i.Value, UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.MiB)));
                    this.NbrItemsParsed += devices.Count;
                    nbrGenerated += (uint)devices.Count;
                }
            }

            {
                var percentage = jObject.TryGetValue("percentage");

                if (percentage != null)
                {
                    ++this.NbrItemsParsed;

                    var devices = percentage.ToObject<Dictionary<string, decimal>>();

                    this.Node.Machine.Devices.PercentUtilized = devices.Select(i => new KeyValuePair<string, decimal>(i.Key, i.Value / 100));
                    this.NbrItemsParsed += devices.Count;
                    nbrGenerated += (uint) devices.Count;
                }
            }

            this.Processed = true;
            return nbrGenerated;
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }
    }
}
