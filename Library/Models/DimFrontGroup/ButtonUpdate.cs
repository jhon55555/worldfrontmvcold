using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public class ButtonUpdate
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "buttonid is empty.")]
        public Int32 bid { get; set; }
        [Required(ErrorMessage = "buttontext is empty.")]
        public String btxt { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "buttonvalue is empty.")]
        public Int64 bval { get; set; }
        public Int32 uid { get; set; }
    }
}
