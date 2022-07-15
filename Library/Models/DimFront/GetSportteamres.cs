using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public class GetSportteamres
    {
        public string sportid { get; set; }
        public string eid { get; set; }
        public string ename { get; set; }
        public string tm1 { get; set; }
        public string tm2 { get; set; }
        public string dt { get; set; }
    }
}
