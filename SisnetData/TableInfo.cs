using System.Collections.Generic;

namespace SisnetData
{
    public class TableInfo
    {
        public string Name;
        public string SizeInfo;
        public long Size;
        public long Count;
        public List<string> Keys { get; internal set; } = new List<string>();

        public override string ToString()
        {
            return this.Name + "[" + string.Join(", ", this.Keys.ToArray()) + "]"; ;
        }
    }
}
