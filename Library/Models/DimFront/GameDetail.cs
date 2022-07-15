using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class GameDetail
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int64 gmid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "eventtypeid is empty.")]
        public Int32 etid { get; set; }
    }
    public class GameDetailOpen
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int64 gmid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "eventtypeid is empty.")]
        public Int32 etid { get; set; }
        public String tablename { get; set; }
    }
}
