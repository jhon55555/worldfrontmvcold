using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class Tvata
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int64 gmid { get; set; }
        [Required(ErrorMessage = "mobiletype is empty.")]
        public String mtype { get; set; }
    }
    public partial class Gethighlightdata
    {
        [Required(ErrorMessage = "key is empty.")]
        public String key { get; set; }
    }
    public class Roothlho
    {
        public int i { get; set; }
        public string n { get; set; }
        public bool d { get; set; }
    }
}
