using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class TVUrl
    {
        public Guid guid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty")]
        public Int64 gmid { get; set; }
        [Required(ErrorMessage = "platform is empty.")]
        public String platform { get; set; }
        [Required(ErrorMessage = "ip is empty.")]
        public String ipa { get; set; }
    }
}
