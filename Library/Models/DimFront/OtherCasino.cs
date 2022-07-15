using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class OtherCasino
    {
        public Boolean mobile { get; set; }
        [Required(ErrorMessage = "casinotypeid is empty.")]
        public String ctype { get; set; }
        //[Range(1, Int16.MaxValue, ErrorMessage = "istest is empty.")]
        public Int16 istest { get; set; }
    }
}
