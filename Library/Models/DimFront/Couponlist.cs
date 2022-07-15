using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFront
{
    public class Couponlist
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "aid is empty.")]
        public Int64 aid { get; set; }
        public Int64 uid { get; set; }
        [Range(1, Double.MaxValue, ErrorMessage = "amount is empty.")]
        public Double amt { get; set; }
        public String copcod { get; set; }
    }
    public class Createwallet
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "aid is empty.")]
        public Int64 aid { get; set; }
        public Int64 uid { get; set; }
        [Range(1, Double.MaxValue, ErrorMessage = "amount is empty.")]
        public Double amt { get; set; }
        [Required(ErrorMessage = "copid is empty.")]
        public String copid { get; set; }
        [Required(ErrorMessage = "DepositDate is empty.")]
        public String ddt { get; set; }
    }
    public class Couponlistapi
    {
        public String DepositId { get; set; }
        public String DepositNumber { get; set; }
        public Double DepositAmount { get; set; }
        public String PlayerId { get; set; }
        public String PlayerName { get; set; }
        public Double Balance { get; set; }
        public String CouponId { get; set; }
        public String SubId { get; set; }
        public Double SubBalance { get; set; }
        public DateTime DepositDate { get; set; }
    }
    public class Coulst
    {
        public string id { get; set; }
        public string name { get; set; }
        public double wagering { get; set; }
        public int expiresIn { get; set; }
        public double amount { get; set; }
        public double minBalance { get; set; }
        public int depositnumber { get; set; }
        public string product { get; set; }
    }

    public class CouponlistRes
    {
        public List<Coulst> data { get; set; }
        public string error { get; set; }
        public int errorcode { get; set; }
    }
    public class crw
    {
        public string Id { get; set; }
        public string ClientId { get; set; }
        public string PlayerId { get; set; }
        public string PlayerName { get; set; }
        public string DepositId { get; set; }
        public string CouponId { get; set; }
        public double Wagering { get; set; }
        public double RemainingWagering { get; set; }
        public double Amount { get; set; }
        public double MinBalance { get; set; }
        public DateTime Given { get; set; }
        public int Status { get; set; }
        public DateTime ExpireDate { get; set; }
        public DateTime DepositDate { get; set; }
        public double DepositAmount { get; set; }
        public int DepositNumber { get; set; }
        public string DepositGroupId { get; set; }
        public string DepositGroupName { get; set; }
        public string DepositCouponId { get; set; }
        public string DepositCouponName { get; set; }
        public string ProductId { get; set; }
        public string ProductName { get; set; }
        public string ProductKey { get; set; }
        public DateTime CreateDate { get; set; }
        public object UpdateDate { get; set; }
        public object DeleteDate { get; set; }
    }
    public class CreatewalletRes
    {
        public crw Data { get; set; }
        public String Error { get; set; }
        public int ErrorCode { get; set; }
    }
    public class Getcophistory
    {
        public String PlayerId { get; set; }
    }
    public class ghcop
    {
        public string Id { get; set; }
        public string ClientId { get; set; }
        public string PlayerId { get; set; }
        public string PlayerName { get; set; }
        public string DepositId { get; set; }
        public string CouponId { get; set; }
        public double Wagering { get; set; }
        public double RemainingWagering { get; set; }
        public double Amount { get; set; }
        public double MinBalance { get; set; }
        public DateTime Given { get; set; }
        public int Status { get; set; }
        public DateTime CreateDate { get; set; }
        public DateTime ExpireDate { get; set; }
        public DateTime DepositDate { get; set; }
        public double DepositAmount { get; set; }
        public int DepositNumber { get; set; }
        public string DepositGroupId { get; set; }
        public string DepositGroupName { get; set; }
        public string DepositCouponId { get; set; }
        public string DepositCouponName { get; set; }
        public string ProductId { get; set; }
        public string ProductName { get; set; }
        public string ProductKey { get; set; }
    }

    public class GetcophistoryRes
    {
        public int TotalCount { get; set; }
        public List<ghcop> Data { get; set; }
        public string Error { get; set; }
        public int ErrorCode { get; set; }
    }

    public class Datasub
    {
        public Double amount { get; set; }
    }

    public class Subres
    {
        public Datasub data { get; set; }
        public string error { get; set; }
        public int errorCode { get; set; }
    }
}
