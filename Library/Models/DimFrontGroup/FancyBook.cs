using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public class FancyBook
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int64 gmid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "marketid is empty.")]
        public Int64 mid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "sectionid is empty.")]
        public Int64 sid { get; set; }
        public Int32 uid { get; set; }
        [Required(ErrorMessage = "type is empty.")]
        public String type { get; set; }
    }
}
