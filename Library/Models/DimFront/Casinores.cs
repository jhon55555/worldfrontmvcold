using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class Casinores
    {
        [Required(ErrorMessage = "date is empty.")]
        public String dt { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
    public partial class CheckCasinovs
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        public Int16 istest { get; set; }
    }
}
