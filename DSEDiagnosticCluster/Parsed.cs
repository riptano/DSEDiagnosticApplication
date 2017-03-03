﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public interface IParsed
    {
        IPath Path { get; }
        Cluster Cluster { get; }
        IDataCenter DataCenter { get; }
        INode Node { get; }
        int Items { get; }
        uint LineNbr { get; }
	}
}
