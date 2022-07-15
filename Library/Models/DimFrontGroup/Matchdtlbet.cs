using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Matchdtlbet
    {
        [Required(ErrorMessage = "type is empty.")]
        public String type { get; set; }
        public Int32 uid { get; set; }
        [Range(1, 5, ErrorMessage = "enter correct gtype.")]
        public int gtype { get; set; }
    }
}
