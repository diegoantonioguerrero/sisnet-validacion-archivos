using SisnetDBComparer.Dto;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace SisnetDBComparer.Utils
{
    // Clase auxiliar para comparar los elementos del ListView durante la ordenación
    public class ListViewItemComparer : IComparer
    {
        private int column;
        private SortOrder sortOrder;
        List<ItemDTO> totalData;

        public int Column
        {
            get { return this.column; }
        }
        private ColIndexComparer ColIndex
        {
            get
            {

                return (ColIndexComparer)column;
            }
        }


        public ListViewItemComparer(int column, List<ItemDTO> totalData, SortOrder sortOrder = SortOrder.Ascending)
        {
            this.column = column;
            this.totalData = totalData;
        }

        public void ToggleSortOrder()
        {
            sortOrder = (sortOrder == SortOrder.Ascending) ? SortOrder.Descending : SortOrder.Ascending;
        }

        public int Compare(object x, object y)
        {
            ListViewItem lvStart;
            ListViewItem lvEnd;
            if (sortOrder == SortOrder.Ascending)
            {
                lvStart = (ListViewItem)x;
                lvEnd = (ListViewItem)y;
            }
            else
            {
                lvStart = (ListViewItem)y;
                lvEnd = (ListViewItem)x;
            }

            string index1 = lvStart.SubItems[(int)ColIndexComparer.Index].Text;
            string index2 = lvEnd.SubItems[(int)ColIndexComparer.Index].Text;
            ItemDTO item1 = this.totalData[int.Parse(index1) - 1];
            ItemDTO item2 = this.totalData[int.Parse(index2) - 1];

            if (this.ColIndex == ColIndexComparer.Size1)
            {
                return item1.Table1SizeNum.CompareTo(item2.Table1SizeNum);
            }
            else if (this.ColIndex == ColIndexComparer.Size2)
            {
                return item1.Table2SizeNum.CompareTo(item2.Table2SizeNum);
            }
            else if (this.ColIndex == ColIndexComparer.Counter1)
            {
                return item1.CountTable1.CompareTo(item2.CountTable1);
            }
            else if (this.ColIndex == ColIndexComparer.Counter2)
            {
                return item1.CountTable2.CompareTo(item2.CountTable2);
            }
            else if (this.ColIndex == ColIndexComparer.Index)
            {
                return item1.Index.CompareTo(item2.Index);
            }
            else
            {
                index1 = lvStart.SubItems[column].Text;
                index2 = lvEnd.SubItems[column].Text;
                return String.Compare(index1, index2);
            }


        }
    }
}
