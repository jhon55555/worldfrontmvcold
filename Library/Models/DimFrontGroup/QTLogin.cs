using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class QTLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        //[Required(ErrorMessage = "gamename is empty.")]
        public String gamename { get; set; }
        [Required(ErrorMessage = "device is empty.")]
        public String device { get; set; }
        public String rurl { get; set; }
        //[Required(ErrorMessage = "gid is empty.")]
        public String gid { get; set; }
    }
}
