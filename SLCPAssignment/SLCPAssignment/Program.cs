using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SLCPAssignment
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new Slcp().Start();
        }
    }

    public class Slcp
    {
        #region private vars

        private const string SlcpQuery = @"SELECT [zipcode] ,[rate] FROM[SLCSP].[dbo].[slcsp]";
        private const string TempQuery = @"SELECT *  FROM 
                                        (SELECT s.[zipcode], z.rate_area, z.state
                                          FROM [SLCSP].[dbo].[slcsp] as s
                                           INNER JOIN [SLCSP].[dbo].[zips] as z
                                          ON z.zipcode = s.zipcode) as temp
                                        LEFT JOIN (
                                          SELECT 
                                               [plan_id]
                                              ,[state]
                                              ,[metal_level]
                                              ,[rate]
                                              ,[rate_area]
                                          FROM [SLCSP].[dbo].[plans]
                                          WHERE metal_level = 'Silver') as temp2
                                          ON temp.state = temp2.state and temp.rate_area = temp2.rate_area
                                         WHERE zipcode not in ( SELECT DISTINCT t.zipcode FROM [SLCSP].[dbo].[slcsp] as t WHERE t.zipcode in (
                                        SELECT  t1.zipcode
                                          FROM [SLCSP].[dbo].[zips] t1
                                          JOIN [SLCSP].[dbo].[zips] t2
                                            on (t1.zipcode = t2.zipcode and t1.county_code <> t2.county_code and t1.rate_area <> t2.rate_area )))  	
                                          ORDER BY zipcode;";
        private readonly DataTable _slcpDataTable;
        private readonly DataTable _tempQueryTable;

        #endregion private vars

        #region properties
        #endregion properties

        public Slcp()
        {
            _slcpDataTable = GetDataTable(SlcpQuery);
            _tempQueryTable = GetDataTable(TempQuery);
        }
        public void Start()
        {
            foreach (DataRow row in _slcpDataTable.Rows)
            {
                DataTable resTable;
                var zip = row["zipcode"].ToString();
                var resData = _tempQueryTable.AsEnumerable().Where(r => r.Field<string>("zipcode").Contains(zip));
                if (resData.Any())
                {
                    resTable = resData.CopyToDataTable();
                    if (resData.Count() > 1)//figure out how to handle this
                    {
                        var minRate = resTable.Compute("MIN([rate])", "");
                        if (minRate == DBNull.Value) continue;
                        resTable.Select("rate = " + minRate)[0].Delete();
                        minRate = (string)resTable.Compute("MIN([rate])", "");
                        _slcpDataTable.Select("zipcode =" + zip)[0][1] = minRate;
                        row["rate"] = minRate;
                    }
                   
                   
                }
            }

            Console.WriteLine();
            Console.ReadLine();
        }

        public DataTable GetDataTable(string sql)
        {
            String connString = ConfigurationManager.ConnectionStrings["SlcpDatabase"].ConnectionString;
            SqlConnection conn = new SqlConnection(connString);
            SqlDataAdapter adapter = new SqlDataAdapter();
            adapter.SelectCommand = new SqlCommand(sql, conn);

            DataTable myDataTable = new DataTable();

            conn.Open();
            try
            {
                adapter.Fill(myDataTable);
            }
            finally
            {
                conn.Close();
            }
            return myDataTable;
        }
    }
}
