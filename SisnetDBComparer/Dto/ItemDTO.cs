using SisnetDBComparer.Utils;
using System.Collections.Generic;

namespace SisnetDBComparer.Dto
{
    public class ItemDTO
    {

        public Status Status { get; set; }
        public int Index { get; set; }
        public string Table1 { get; set; }
        public long CountTable1 { get; set; }
        public string Table2 { get; set; }
        public long CountTable2 { get; set; }
        public string Table1Size { get; internal set; }
        public string Table2Size { get; internal set; }
        public List<string> Table1Keys { get; internal set; }
        public List<string> Table2Keys { get; internal set; }
        public bool? EqualData { get; internal set; }
        public long Table1SizeNum { get; internal set; }
        public long Table2SizeNum { get; internal set; }
        public bool LoadedOnlySchema { get; internal set; }

        public override string ToString()
        {
            return (string.IsNullOrEmpty(this.Table1) ? this.Table2 : this.Table1) + (
                "[" + (string.IsNullOrEmpty(this.Table1) ? this.Table1SizeNum : this.Table2SizeNum) + "]"
                );
        }
    }
}
