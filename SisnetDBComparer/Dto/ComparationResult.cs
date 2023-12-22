using SisnetDBComparer.Utils;

namespace SisnetDBComparer.Dto
{
    public class ComparationResult
    {
        public StatusDetails StatusDetails { get; set; }
        public int Index { get; set; }
        public ItemDTO Item { get; internal set; }


    }
}
