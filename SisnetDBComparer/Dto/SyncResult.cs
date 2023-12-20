using SisnetDBComparer.Utils;

namespace SisnetDBComparer.Dto
{
    public class SyncResult
    {
        public StatusSync StatusSync { get; set; }
        public int Index { get; set; }
        public ItemDTO Item { get; internal set; }
    }
}
