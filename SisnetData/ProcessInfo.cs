using System;
using System.Collections.Generic;
using System.Text;

namespace SisnetData
{
    public class ProcessInfo
    {
        // Fields
        public string mensajeerror;
        public string etiqueta1;
        public string etiqueta2;
        public string etiqueta3;
        public string etiqueta4;

        // Methods
        public override string ToString()
        {
            return string.Concat(new string[] { this.fldidvalidacionarchivos.ToString(), "[", this.nombrearchivoarchivo, "] -> [", this.nombrearchivoarchivoresultante, "]" });
        }

        // Properties
        public string procesarexcel { get; set; }

        public bool ProcesarExcel
        {
            get
            {
                if (string.IsNullOrEmpty(this.procesarexcel))
                {
                    return true;
                }
                if (!this.nombrearchivoarchivo.ToLower().EndsWith("xlsx") && !this.nombrearchivoarchivo.ToLower().EndsWith("xls"))
                {
                    return true;
                }
                return this.procesarexcel.Equals("si");
            }
        }
        public int fldidvalidacionarchivos { get; set; }

        public DateTime fecha { get; set; }

        public string tipo { get; set; }

        public string numeroidentificacion { get; set; }

        public byte[] ArchivoData { get; set; }

        public string nombrearchivoarchivo { get; set; }

        public string etiqueta { get; set; }

        public string accion { get; set; }

        public decimal calidadpdf { get; set; }

        public string estado { get; set; }

        public byte[] archivoresultante { get; set; }

        public string nombrearchivoarchivoresultante { get; set; }

        public string extensionarchivoresultant { get; set; }

        public string extensionarchivo { get; set; }


    }


}
