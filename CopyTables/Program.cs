using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;

namespace CopyTables
{
    class Program
    {
        public static string conn2014 = "Data Source=man-gkontrol-1;Initial Catalog=GccMeasurements;Persist Security Info=True;User ID=sa;Password=GcC;Connection Timeout=0";
        public static string conn2015 = "Data Source=man-gkontrol-1;Initial Catalog=2015Data;Persist Security Info=True;User ID=sa;Password=GcC;Connection Timeout=0";
        public static DateTime start = DateTime.Now;

        static void Main(string[] args)
        {
            WriteInfo(String.Format("start {0}", GetTime()));
            //DataTable dt = GetAllUnits();
            DataTable dt = GetUnitsGreaterThan(3421);
            //DataTable dt = GetAllUnits(2434);
            WriteInfo(String.Format("Got All Units {0}", GetTime()));

            //WriteInfo("*************    ERRORS    ****************");
            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    DoBulkErrors(unit_id);
            //}

            //WriteInfo("*************    EVENTS    ****************");
            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    DoBulkEvents(unit_id);
            //}

            //WriteInfo("******** OPERATING SITUATIONS    ***********");
            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    DoBulkOperatingSituation(unit_id);
            //}

            //WriteInfo("*************    STATUS    ****************");
            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    DoBulkStatus(unit_id);
            //}

            WriteInfo("************ MEASUREMENTS   ***************");
            foreach (DataRow dr in dt.Rows)
            {
                short unit_id = Convert.ToInt16(dr["unit_id"]);
                DoBulkMeasurements(unit_id, new DateTime(2015, 1, 1));
            }

            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    try
            //    {
            //        DoMeasurements(unit_id, new DateTime(2015, 1, 1));
            //    }
            //    catch (Exception ex)
            //    {
            //        WriteInfo(sw, String.Format("Error {0} on unit {1}", ex.Message, unit_id));
            //    }
            //}

            //code for extra data if does not complete in a day
            //Get latest record for 2015
            //foreach (DataRow dr in dt.Rows)
            //{
            //    short unit_id = Convert.ToInt16(dr["unit_id"]);
            //    DateTime latest2015 = GetLatest2015Record(unit_id);
            //    if (latest2015 > new DateTime(2015, 1, 1))
            //    {
            //        DoMeasurements(unit_id, latest2015);
            //    }
            //    else
            //    {
            //        DoMeasurements(unit_id, new DateTime(2015,1,1));
            //    }
            //}

            WriteInfo(String.Format("End {0}", GetTime()));
            Console.ReadLine();
        }

        private static void DoBulkStatus(short unit_id)
        {
            DataTable dt = new DataTable();
            WriteInfo(string.Format("get status for unit {0} {1}", unit_id, GetTime()));
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
Select * from status where st_timestamp >= '1-jan-2015' and st_unit_ID = @ID
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@ID", Convert.ToInt16(unit_id));
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);

                WriteInfo(string.Format("got {0} rows for unit {1} {2}", dt.Rows.Count, unit_id, GetTime()));
                Thread.Sleep(1000);
                WriteInfo(string.Format("Inserting rows for unit {0} {1}", unit_id, GetTime()));
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn2015))
                {
                    bulkCopy.DestinationTableName = "status";
                    bulkCopy.WriteToServer(dt);
                }
            }
        }

        private static void DoBulkOperatingSituation( int unit_id)
        {
            //1st go do all
            DataTable dt = new DataTable();
            WriteInfo(string.Format("get operatingsituations for unit {0} {1}", unit_id, GetTime()));
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
Select * from OperatingSituations where OS_timestamp >= '1-jan-2015' and OS_unit_ID = @ID
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@ID", Convert.ToInt16(unit_id));
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);

                WriteInfo(string.Format("got {0} rows for unit {1} {2}", dt.Rows.Count, unit_id, GetTime()));
                Thread.Sleep(1000);
                WriteInfo(string.Format("Inserting rows for unit {0} {1}", unit_id, GetTime()));
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn2015))
                {
                    bulkCopy.DestinationTableName = "operatingsituations";
                    bulkCopy.WriteToServer(dt);
                }
            }
        }

        private static void DoBulkEvents( short unit_id)
        {
            //1st go do all
            DataTable dt = new DataTable();
            WriteInfo(String.Format("get events for unit {0} {1}", unit_id, GetTime()));
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
Select * from events where E_timestamp >= '1-jan-2015' and E_unit_ID = @ID
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@ID", Convert.ToInt16(unit_id));
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);

                WriteInfo(string.Format("got {0} rows for unit {1} {2}", dt.Rows.Count, unit_id, GetTime()));
                Thread.Sleep(1000);
                WriteInfo(string.Format("Inserting rows for unit {0} {1}", unit_id, GetTime()));
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn2015))
                {
                    bulkCopy.DestinationTableName = "events";
                    try
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    catch
                    {
                        WriteInfo(String.Format("!!!!******* FAILED Inserting rows for unit {0} {1}", unit_id, GetTime()));
                    }
                }
            }
        }

        private static void DoBulkErrors( int unit_id)
        {
            //1st go do all
            DataTable dt = new DataTable();
            WriteInfo(string.Format("get errors for unit {0} {1}", unit_id, GetTime()));
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
Select * from errors where ER_timestamp >= '1-jan-2015' and ER_unit_ID = @ID
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@ID", Convert.ToInt16(unit_id));
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);

                WriteInfo(string.Format("got {0} rows for unit {1} {2}", dt.Rows.Count, unit_id, GetTime()));
                Thread.Sleep(1000);
                WriteInfo(string.Format("Inserting rows for unit {0} {1}", unit_id, GetTime()));
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn2015))
                {
                    bulkCopy.DestinationTableName = "errors";
                    try
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    catch
                    {
                        WriteInfo(String.Format("!!!!******* FAILED Inserting rows for unit {0} {1}", unit_id, GetTime()));
                    }
                }
            }
        }

        private static void DoBulkMeasurements(int unit_id, DateTime startDate)
        {
            //1st go do all
            DataTable dt = new DataTable();
            WriteInfo(string.Format("get measurements for unit {0} {1}", unit_id, GetTime()));
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
Select * from Measurements where M_timestamp >= @SDATE and M_unit_ID = @ID
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@SDATE", startDate);
                cmd.Parameters.AddWithValue("@ID", Convert.ToInt16(unit_id));
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
                WriteInfo(String.Format("got {0} rows for unit {1} {2}", dt.Rows.Count, unit_id, GetTime()));
                Thread.Sleep(1000);
                WriteInfo(String.Format("Inserting rows for unit {0} {1}", unit_id, GetTime()));
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn2015))
                {
                    bulkCopy.DestinationTableName = "Measurements";
                    bulkCopy.BulkCopyTimeout = 0;
                    try
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    catch
                    {
                        WriteInfo(String.Format("!!!!******* FAILED Inserting rows for unit {0} {1}", unit_id, GetTime()));
                    }
                }
            }
        }

        private static DateTime GetLatest2015Record(short unit_id)
        {
            DateTime latest = new DateTime();
            using (SqlConnection conn = new SqlConnection(conn2015))
            {
                string dbquery = @"
select TOP 1 M_Timestamp
  from measurements
 where M_Unit_ID = @UID
   and M_TimeStamp >= '1-jan-2015'
order by M_TimeStamp desc
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", Convert.ToInt16(unit_id));
                conn.Open();
                SqlDataReader sdr = cmd.ExecuteReader();
                if (sdr.Read())
                {
                    latest = Convert.ToDateTime(sdr[0]);
                }

            }
            return latest;
        }

        private static void WriteInfo(string info)
        {
            StreamWriter sw = new StreamWriter(@"C:\temp\copy.log", true);
            Console.WriteLine(info);
            sw.WriteLine(info);
            sw.Close();
        }

        private static void DoMeasurements(short unit_id, DateTime startdate)
        {
            Console.WriteLine("getting Measurements for unit {0} {1} after {2}", unit_id, GetTime(), startdate);
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select M_Unit_Id, M_LN_Id, M_Timestamp, M_Type, M_Value
  from measurements
 where M_Unit_ID = @UID
   and M_TimeStamp > @SDATE
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", Convert.ToInt16(unit_id));
                cmd.Parameters.AddWithValue("@SDATE", startdate);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }

            Console.WriteLine("got {0} Measurements for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());
            Thread.Sleep(10000);
            Console.WriteLine("Inserting Measurements for unit {0} {1}", unit_id, GetTime());

            if (dt.Rows.Count > 0)
            {
                using (SqlConnection conn = new SqlConnection(conn2015))
                {
                    conn.Open();
                    foreach (DataRow dr in dt.Rows)
                    {
                        string dbquery = @"
insert into Measurements
    (M_Unit_Id, M_LN_Id, M_Timestamp, M_Type, M_value)
values (@UID, @LNID, @STAMP, @TYPE, @INFO)
";
                        SqlCommand cmd = new SqlCommand(dbquery, conn);
                        cmd.Parameters.AddWithValue("@UID", dr["M_Unit_ID"]);
                        cmd.Parameters.AddWithValue("@LNID", dr["M_LN_Id"]);
                        cmd.Parameters.AddWithValue("@STAMP", dr["M_Timestamp"]);
                        cmd.Parameters.AddWithValue("@TYPE", dr["M_Type"]);
                        cmd.Parameters.AddWithValue("@INFO", dr["M_Value"]);
                        cmd.ExecuteNonQuery();

                    }
                    conn.Close();
                }
                Console.WriteLine("Inserted Measurements for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());
                Thread.Sleep(10000);
            }
        }

        private static void DoOpeartingSituation(short unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select OS_Unit_Id, OS_DOS_LN_Id, OS_Timestamp
  from operatingSituations
 where OS_Unit_ID = @UID
   and OS_TimeStamp >= '1-jan-2015'
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            Console.WriteLine("got {0} OperatingSituations for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());

            if (dt.Rows.Count > 0)
            {
                using (SqlConnection conn = new SqlConnection(conn2015))
                {
                    conn.Open();
                    foreach (DataRow dr in dt.Rows)
                    {
                        string dbquery = @"
insert into operatingSituations
    (OS_Unit_Id, OS_DOS_LN_Id, OS_Timestamp)
values (@UID, @LNID, @STAMP)
";
                        SqlCommand cmd = new SqlCommand(dbquery, conn);
                        cmd.Parameters.AddWithValue("@UID", dr["OS_Unit_ID"]);
                        cmd.Parameters.AddWithValue("@LNID", dr["OS_DOS_LN_Id"]);
                        cmd.Parameters.AddWithValue("@STAMP", dr["OS_Timestamp"]);
                        cmd.ExecuteNonQuery();

                    }
                    conn.Close();
                }
            }
        }

        private static void DoStatus(short unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select ST_Unit_Id, ST_DS_LN_Id, ST_Timestamp
  from status
 where ST_Unit_ID = @UID
   and St_TimeStamp >= '1-jan-2015'
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            Console.WriteLine("got {0} Status for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());

            if (dt.Rows.Count > 0)
            {
                using (SqlConnection conn = new SqlConnection(conn2015))
                {
                    conn.Open();
                    foreach (DataRow dr in dt.Rows)
                    {
                        string dbquery = @"
insert into status
    (ST_Unit_Id, ST_DS_LN_Id, ST_Timestamp)
values (@UID, @LNID, @STAMP)
";
                        SqlCommand cmd = new SqlCommand(dbquery, conn);
                        cmd.Parameters.AddWithValue("@UID", dr["ST_Unit_ID"]);
                        cmd.Parameters.AddWithValue("@LNID", dr["ST_DS_LN_Id"]);
                        cmd.Parameters.AddWithValue("@STAMP", dr["ST_Timestamp"]);
                        cmd.ExecuteNonQuery();

                    }
                    conn.Close();
                }
            }
        }

        private static void DoEvents(short unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select E_Unit_Id, E_DE_LN_Id, E_Timestamp
  from events
 where E_Unit_ID = @UID
   and E_TimeStamp >= '1-jan-2015'
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            Console.WriteLine("got {0} events for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());

            if (dt.Rows.Count > 0)
            {
                using (SqlConnection conn = new SqlConnection(conn2015))
                {
                    conn.Open();
                    foreach (DataRow dr in dt.Rows)
                    {
                        string dbquery = @"
insert into events
    (E_Unit_Id, E_DE_LN_Id, E_Timestamp)
values (@UID, @LNID, @STAMP)
";
                        SqlCommand cmd = new SqlCommand(dbquery, conn);
                        cmd.Parameters.AddWithValue("@UID", dr["E_Unit_ID"]);
                        cmd.Parameters.AddWithValue("@LNID", dr["E_DE_LN_Id"]);
                        cmd.Parameters.AddWithValue("@STAMP", dr["E_Timestamp"]);
                        cmd.ExecuteNonQuery();

                    }
                    conn.Close();
                }
            }
        }

        private static void DoErrors(short unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select ER_Unit_Id, ER_DER_LN_Id, ER_Timestamp
  from errors
 where ER_Unit_ID = @UID
   and ER_TimeStamp >= '1-jan-2015'
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            Console.WriteLine("got {0} errors for unit {1} {2}", dt.Rows.Count, unit_id, GetTime());

            if (dt.Rows.Count > 0)
            {
                using (SqlConnection conn = new SqlConnection(conn2015))
                {
                    conn.Open();
                    foreach (DataRow dr in dt.Rows)
                    {
                        string dbquery = @"
insert into errors
    (ER_Unit_Id, ER_DER_LN_Id, ER_Timestamp)
values (@UID, @LNID, @STAMP)
";
                        SqlCommand cmd = new SqlCommand(dbquery, conn);
                        cmd.Parameters.AddWithValue("@UID", dr["ER_Unit_ID"]);
                        cmd.Parameters.AddWithValue("@LNID", dr["ER_DER_LN_Id"]);
                        cmd.Parameters.AddWithValue("@STAMP", dr["ER_Timestamp"]);
                        cmd.ExecuteNonQuery();

                    }
                    conn.Close();
                }
            }
        }

        private static DataTable GetAllUnits()
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select unit_id
  from unit
order by unit_id
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            return dt;
        }

        private static DataTable GetAllUnits(int unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select unit_id
  from unit
where unit_id = @UID
order by unit_id
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            return dt;
        }

        private static DataTable GetUnitsGreaterThan(int unit_id)
        {
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(conn2014))
            {
                string dbquery = @"
select unit_id
  from unit
where unit_id > @UID
order by unit_id
";
                SqlCommand cmd = new SqlCommand(dbquery, conn);
                cmd.Parameters.AddWithValue("@UID", unit_id);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dt);
            }
            return dt;
        }

        private static TimeSpan GetTime()
        {
            return DateTime.Now.Subtract(start);
        }
    }
}
