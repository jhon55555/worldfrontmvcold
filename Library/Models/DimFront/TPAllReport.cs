using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class TPAllReport
    {
        public Int32 uid { get; set; }
        [Required(ErrorMessage = "date is empty.")]
        public String dt { get; set; }
        [Required(ErrorMessage = "casinotype is empty.")]
        public String ctype { get; set; }
    }
}
