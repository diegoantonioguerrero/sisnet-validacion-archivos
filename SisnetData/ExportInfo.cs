using System;
using System.Collections.Generic;
using System.Text;

namespace SisnetData
{
    public class ExportInfo
    {
        // Properties
        public string Consecutivo { get; set; }

        public string ArchivoName { get; set; }

        public string ArchivoLength { get; set; }

        public byte[] ArchivoData { get; set; }

    }
}
