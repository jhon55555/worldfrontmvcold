using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class CasinoUserBook
    {
        public Int32 uid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "marketid is empty.")]
        public Int64 mid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
    public partial class KbcPayout
    {
        public Int32 uid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "marketid is empty.")]
        public Int64 mid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "pid is empty.")]
        public Int64 pid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        [Required(ErrorMessage = "statement is empty.")]
        public String sta { get; set; }
    }
}
