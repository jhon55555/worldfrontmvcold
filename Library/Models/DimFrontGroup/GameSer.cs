using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public class GameSer
    {
        [Required(ErrorMessage = "ename is empty.")]
        public String ename { get; set; }
    }
}
