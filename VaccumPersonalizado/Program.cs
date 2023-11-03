using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;


namespace vaccumPersonalizado
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 4 || args[0] != "-f" || args[2] != "-d")
            {
                Console.WriteLine("Uso: MiPrograma.exe -f \"archivo.txt\" -d databaseName");
                return;
            }

            string filePath = args[1];
            string databaseName = args[3];

            try
            {
                FileProcessor fp = new FileProcessor();
                fp.ProcesarArchivo(databaseName, filePath);

              
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer el archivo o conectar a la base de datos: {ex.Message}");
            }
        }
    }
}
