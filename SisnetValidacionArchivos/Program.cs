namespace SisnetServiceConversor
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            (new FileConversorService()).Start(null);

        }
    }
}
