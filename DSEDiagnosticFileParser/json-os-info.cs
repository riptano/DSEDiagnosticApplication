﻿using System;
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
    internal sealed class json_os_info : ProcessJsonFile
    {
        public json_os_info(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node)
			: base(catagory, diagnosticDirectory, file, node)
		{
        }

        public override uint ProcessJSON(JObject jObject)
        {
            jObject.TryGetValue("sub_os").NullSafeSet<string>(c => this.Node.Machine.OS = c);
            jObject.TryGetValue("os_version").NullSafeSet<string>(c => this.Node.Machine.OSVersion = new Version(c));

            return 1;
        }

        public override IResult GetResult()
        {
            throw new NotImplementedException();
        }
    }
}
