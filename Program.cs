using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MissingFilesEPF
{
    class Program
    {
        static List<etf_epf_record> ResultListEPF = new List<etf_epf_record>();
        static List<etf_epf_record> ResultListETF = new List<etf_epf_record>();

        static void Main(string[] args)
        {

            //Get Directory Informations of the Root directory of the Default Files upload folder     
            //string filter = "('CBCEPF96972','CBCEPF96978','CBCEPF97052','CBCEPF97871','CBCEPF97885','CBCEPF98163','CBCEPF98166')";
            List<etf_epf_record> RefListEPF = GetListEPF();
            List<etf_epf_record> RefListETF = GetListETF();
            List<filePath> AllFilePathsEPF = GetAllFileList(@"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_ETF");
            List<filePath> AllFilePathsETF = GetAllFileList(@"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_ETF");
            int mode = 1; // mode=0 Match reference with file name, mode=1 Match reference with file content


            switch (mode)
            {
                case 0: // mode=0 Match reference with file name
                    //EPF
                    foreach (var tran in RefListEPF)
                    {
                        List<filePath> has = AllFilePathsEPF.FindAll(file => file.fullPath.Contains(tran.etref));

                        if (has.Count > 0)
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            foreach (var item in has)
                            {
                                record.files += item.fullPath + ",";
                            }
                            ResultListEPF.Add(record);
                        }
                        else
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            ResultListEPF.Add(record);
                        }
                    }

                    //ETF
                    foreach (var tran in RefListETF)
                    {
                        List<filePath> has = AllFilePathsETF.FindAll(file => file.fullPath.Contains(tran.etref));

                        if (has.Count > 0)
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            foreach (var item in has)
                            {
                                record.files += item.fullPath + ",";
                            }
                            ResultListETF.Add(record);
                        }
                        else
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            ResultListETF.Add(record);
                        }
                    }
                    break;
                case 1: //mode=1 Match reference with file content

                    //EPF
                    foreach (var tran in RefListEPF)
                    {
                        List<filePath> has = AllFilePathsEPF.FindAll(file => file.fullPath.Contains(tran.etref));

                        if (has.Count > 0)
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            foreach (var item in has)
                            {
                                if (item.fullPath.Contains("EVEMP"))  //EVEMC CBCEPF  EVEMP
                                {
                                    FileStream _fs = new FileStream(item.fullPath, FileMode.Open, FileAccess.Read);
                                    Byte[] fileByte = new byte[_fs.Length];
                                    _fs.Read(fileByte, 0, System.Convert.ToInt32(_fs.Length));

                                    try
                                    {
                                        //string[] result = GetEpfFileInformation(_fs, tran.etreg, tran.etfrpd.Substring(0, 4), tran.etfrpd.Substring(4, 2), tran.etamt, tran.etnomb);
                                        string[] result = GetEpfPaymentFileInformation(_fs, tran.etreg, tran.etfrpd, tran.etamt, tran.etnomb);
                                        record.summary += result[0] + "^" + result[1] + ",";
                                    }
                                    catch (Exception ex)
                                    {
                                        record.errors += ex.Message + "^";
                                    }
                                    record.files += item.fullPath + "^";
                                    _fs.Close();
                                    _fs.Dispose();
                                }
                                //else
                                //{
                                //    record.files += item.fullPath + "^";
                                //}
                            }

                            ResultListEPF.Add(record);
                        }
                        else
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            ResultListEPF.Add(record);
                        }
                    }

                    //ETF
                    foreach (var tran in RefListETF)
                    {
                        List<filePath> has = AllFilePathsETF.FindAll(file => file.fullPath.Contains(tran.etref));

                        if (has.Count > 0)
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            foreach (var item in has)
                            {
                                FileStream _fs = new FileStream(item.fullPath, FileMode.Open, FileAccess.Read);
                                Byte[] fileByte = new byte[_fs.Length];
                                _fs.Read(fileByte, 0, System.Convert.ToInt32(_fs.Length));

                                try
                                {
                                    string[] result = GetEtfFileInformation(_fs, tran.etreg, tran.etfrpd + tran.ettopd, tran.etamt, tran.etnomb, 1);
                                    record.summary += result[0] + "^" + result[1] + ",";
                                }
                                catch (Exception ex)
                                {
                                    record.errors += ex.Message + "^";
                                }

                                record.files += item.fullPath + "^";

                                _fs.Close();
                                _fs.Dispose();

                            }
                            ResultListETF.Add(record);
                        }
                        else
                        {
                            etf_epf_record record = tran;
                            record.count = has.Count;
                            ResultListETF.Add(record);
                        }
                    }

                    break;
            }


            var logPath = @"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_ETF";
            logPath = logPath + "/" + System.DateTime.Now.ToString("yyyyMMdd HHmmss") + "EPF.csv";
            if (string.IsNullOrEmpty(logPath)) return;
            var fs = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            var sw = new StreamWriter(fs);

            foreach (var item in ResultListEPF)
            {
                sw.WriteLine(item.etref + "," + item.etreg + "," + item.count + "," + item.etnomb + "," + item.etamt + "," + item.etdate + "," + item.ettime + "," + item.etpflg + "," + item.etptyp + "," + item.etcopr + "," + item.summary + "," + item.files + "," + item.errors);
                Console.WriteLine(item.etref + ":" + item.count.ToString());
            }

            sw.Flush();
            sw.Close(); sw.Dispose();
            fs.Close(); fs.Dispose();


            logPath = @"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_ETF";
            logPath = logPath + "/" + System.DateTime.Now.ToString("yyyyMMdd HHmmss") + "ETF.csv";
            if (string.IsNullOrEmpty(logPath)) return;
            var fs1 = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            var sw1 = new StreamWriter(fs1);

            foreach (var item in ResultListETF)
            {

                sw1.WriteLine(item.etref + "," + item.etreg + "," + item.count + "," + item.etnomb + "," + item.etamt + "," + item.etdate + "," + item.ettime + "," + item.etpflg + "," + item.etptyp + "," + item.etcopr + "," + item.summary + "," + item.files + "," + item.errors);
                Console.WriteLine(item.etref + ":" + item.count.ToString());
            }

            sw1.Flush();
            sw1.Close(); sw1.Dispose();

            fs1.Close(); fs1.Dispose();



            //foreach (var item in RefList)
            //{
            //    DirSearch(@"D:\EPFETF\EPFETFdata\EPFETFdata", item);
            //    Console.WriteLine(item.etref + ":" + ResultList.Count.ToString());
            //}
            //Console.WriteLine(ResultList.Count);

        }

        //static void Main(string[] args)
        //{

        //    string filter = "('CBCEPF96972','CBCEPF96978','CBCEPF97052','CBCEPF97871','CBCEPF97885','CBCEPF98163','CBCEPF98166')";
        //    GenerateEPFPaymentFiles(@"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_EPF\Batch_05_20171020", filter);
        //}


        //static void Main(string[] args)
        //{
        //    string filter = "('CBCETF93107',	'CBCETF93108',	'CBCETF93109',	'CBCETF93110',	'CBCETF93263',	'CBCETF93264',	'CBCETF93265',	'CBCETF93266',	'CBCETF93267',	'CBCETF93268',	'CBCETF93340',	'CBCETF93343',	'CBCETF93465',	'CBCETF93575',	'CBCETF93576',	'CBCETF93578',	'CBCETF93973',	'CBCETF93974',	'CBCETF94467',	'CBCETF94468',	'CBCETF94469',	'CBCETF94470',	'CBCETF94471',	'CBCETF94472',	'CBCETF94473',	'CBCETF94474',	'CBCETF94475',	'CBCETF94476',	'CBCETF94477',	'CBCETF94478',	'CBCETF94479',	'CBCETF94480',	'CBCETF94481',	'CBCETF94482',	'CBCETF94483',	'CBCETF94484',	'CBCETF94501')";
        //    CopyandRenameETFContributionFiles(@"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_ETF\Batch_2017_10_16", filter, @"D:\EPFETF\Briyan\ETF\ETF 16102017");
        //}

        //static void Main(string[] args)
        //{
        //    string filter = "('CBCEPF96972','CBCEPF96978','CBCEPF97052','CBCEPF97871','CBCEPF97885','CBCEPF98163','CBCEPF98166')";
        //    RenameEPFContributionFiles(@"D:\EPFETF\Deployments\AUG_2017\AUG_2017_LIVE_EPF\Batch_05_20171020", filter, @"D:\EPFETF\Briyan\EPF\Batch 5 2017_10_20");
        //    //RenameEPFPaymentFiles(@"D:\EPFETF\2017_SEP\SEP_2017_EPF_TO_LIVE", "", @"D:\EPFETF\2017_SEP\SEP_2017_EPF");
        //}

        static List<etf_epf_record> GetListEPF(string filter = "")
        {
            System.Data.Odbc.OdbcConnection l_objConnection = new System.Data.Odbc.OdbcConnection();
            System.Data.Odbc.OdbcCommand l_objCmd = new System.Data.Odbc.OdbcCommand();
            System.Data.Odbc.OdbcDataReader l_objRdr = null;
            l_objConnection.ConnectionString = "DSN=PRODDSN_LIVE;PWD=INTERNET;UID=INTERNET";
            //l_objConnection.ConnectionString = "DSN=PRODDSN;PWD=CX50@12#;UID=INTCOMUSR";
            l_objConnection.Open();
            l_objCmd.Connection = l_objConnection;
            l_objCmd.CommandType = CommandType.Text;

            string sql = string.Empty;
            if (string.IsNullOrEmpty(filter))
            {
                sql = "select etref,etreg,etamt,etpflg,etdate,ettime,etptyp,etfrpd,ettopd,etnomb,etbkcd,etbrcd,etacct,etdcd,etadat,etcopr,ettran from inetprddt1.etp1000 where etdate>='20170801' and etdate<='20170831' and  etref like 'CBCEPF%' order by etdate";
            }
            else
            {
                sql = "select etref,etreg,etamt,etpflg,etdate,ettime,etptyp,etfrpd,ettopd,etnomb,etbkcd,etbrcd,etacct,etdcd,etadat,etcopr,ettran from inetprddt1.etp1000 where etdate>='20170801' and etdate<='20170831' and  etref like 'CBCEPF%' and etref in " + filter + " order by etdate";
            }

            l_objCmd.CommandText = sql;

            if (l_objRdr != null)
            {
                l_objRdr.Close();
            }

            l_objRdr = l_objCmd.ExecuteReader();

            List<etf_epf_record> list = new List<etf_epf_record>();


            if (l_objRdr.HasRows)
            {
                while (l_objRdr.Read())
                {
                    etf_epf_record file = new etf_epf_record();
                    file.etref = l_objRdr["etref"].ToString().Trim();
                    file.etreg = l_objRdr["etreg"].ToString().Trim();
                    file.etamt = l_objRdr["etamt"].ToString().Trim();
                    file.etpflg = l_objRdr["etpflg"].ToString().Trim();
                    file.etdate = l_objRdr["etdate"].ToString().Trim();
                    file.ettime = l_objRdr["ettime"].ToString().Trim();
                    file.etptyp = l_objRdr["etptyp"].ToString().Trim();
                    file.etfrpd = l_objRdr["etfrpd"].ToString().Trim();//From Period
                    file.ettopd = l_objRdr["ettopd"].ToString().Trim();//To Period
                    file.etnomb = l_objRdr["etnomb"].ToString().Trim();//EMployeed   
                    file.etbkcd = l_objRdr["etbkcd"].ToString().Trim();
                    file.etbrcd = l_objRdr["etbrcd"].ToString().Trim();
                    file.etacct = l_objRdr["etacct"].ToString().Trim();
                    file.etdcd = l_objRdr["etdcd"].ToString().Trim();
                    file.etadat = l_objRdr["etadat"].ToString().Trim();
                    file.etcopr = l_objRdr["etcopr"].ToString().Trim();
                    file.ettran = l_objRdr["ettran"].ToString().Trim();

                    list.Add(file);
                }
            }

            return list;
        }

        static List<etf_epf_record> GetListETF(string filter = "")
        {
            System.Data.Odbc.OdbcConnection l_objConnection = new System.Data.Odbc.OdbcConnection();
            System.Data.Odbc.OdbcCommand l_objCmd = new System.Data.Odbc.OdbcCommand();
            System.Data.Odbc.OdbcDataReader l_objRdr = null;
            l_objConnection.ConnectionString = "DSN=PRODDSN_LIVE;PWD=INTERNET;UID=INTERNET";
            l_objConnection.Open();
            l_objCmd.Connection = l_objConnection;
            l_objCmd.CommandType = CommandType.Text;

            string sql = string.Empty;
            if (string.IsNullOrEmpty(filter))
            {
                sql = "select etref,etreg,etamt,etpflg,etdate,ettime,etptyp,etfrpd,ettopd,etnomb,etbkcd,etbrcd,etacct,etdcd,etadat,etcopr,ettran from inetprddt1.etp1000 where etdate>='20170801' and etdate<='20170831' and  etref like 'CBCETF%' order by etdate";
            }
            else
            {
                sql = "select etref,etreg,etamt,etpflg,etdate,ettime,etptyp,etfrpd,ettopd,etnomb,etbkcd,etbrcd,etacct,etdcd,etadat,etcopr,ettran from inetprddt1.etp1000 where etdate>='20170801' and etdate<='20170831' and  etref like 'CBCETF%' and etref in " + filter + " order by etdate";
            }
            
            l_objCmd.CommandText = sql;

            if (l_objRdr != null)
            {
                l_objRdr.Close();
            }
            l_objRdr = l_objCmd.ExecuteReader();

            List<etf_epf_record> list = new List<etf_epf_record>();


            if (l_objRdr.HasRows)
            {
                while (l_objRdr.Read())
                {
                    etf_epf_record file = new etf_epf_record();
                    file.etref = l_objRdr["etref"].ToString().Trim();
                    file.etreg = l_objRdr["etreg"].ToString().Trim();
                    file.etamt = l_objRdr["etamt"].ToString().Trim();
                    file.etpflg = l_objRdr["etpflg"].ToString().Trim();
                    file.etdate = l_objRdr["etdate"].ToString().Trim();
                    file.ettime = l_objRdr["ettime"].ToString().Trim();
                    file.etptyp = l_objRdr["etptyp"].ToString().Trim();
                    file.etfrpd = l_objRdr["etfrpd"].ToString().Trim();//From Period
                    file.ettopd = l_objRdr["ettopd"].ToString().Trim();//To Period
                    file.etnomb = l_objRdr["etnomb"].ToString().Trim();//EMployeed   
                    file.etadat = l_objRdr["etadat"].ToString().Trim();
                    file.etcopr = l_objRdr["etcopr"].ToString().Trim();
                    file.ettran = l_objRdr["ettran"].ToString().Trim();
                    list.Add(file);
                }
            }

            return list;
        }

        //static void DirSearch(string sDir, etf_epf_record record)
        //{
        //    try
        //    {
        //        foreach (string d in Directory.GetDirectories(sDir))
        //        {
        //            foreach (string f in Directory.GetFiles(d))
        //            {
        //                string extension = Path.GetFullPath(f);
        //                if (extension != null && (extension.Contains(record.etref)))
        //                {
        //                    int has = ResultList.FindIndex(cus => cus.etref == record.etref);
        //                    if (has > 0)
        //                    {
        //                        ResultList[has].count = ResultList[has].count + 1;
        //                        ResultList[has].files = ResultList[has].files + extension + "^";
        //                    }
        //                    else
        //                    {
        //                        etf_epf_record item = record;
        //                        record.count = record.count + 1;
        //                        record.files = record.files + extension + "^";
        //                        ResultList.Add(record);
        //                    }

        //                }
        //            }
        //            DirSearch(d, record);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.Message);
        //    }
        //}

        static List<filePath> GetAllFileList(string path)
        {
            List<filePath> FileswithPaths = new List<filePath>();
            var allFiles = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);

            foreach (var file in allFiles)
            {
                FileswithPaths.Add(new filePath { fullPath = file });
            }

            return FileswithPaths;
        }

        static string[] GetEtfFileInformation(Stream objFileStream, string empNo, string toFromDates, string ttlAmount, string noOfEmp, int select)
        {
            objFileStream.Seek(0, SeekOrigin.Begin);
            StreamReader sr = new StreamReader(objFileStream);
            string inputLine = "";
            int l_intRecordCount = 0;
            double d_TotAmount = 0.0d;
            double h_TotAmount = 0.0d;
            string[] FileSummary = new string[2];
            string d_empRegNumber = "";
            string h_empRegNumber = "";
            string d_fromToDates = "";
            string h_fromToDates = "";
            int h_totalRecords = 0;

            while ((inputLine = sr.ReadLine()) != null)
            {
                double amount = 0.0d;
                double h_amount = 0.0d;
                string isDetailLine = inputLine.Substring(0, 1).Trim().ToUpper();

                if (string.IsNullOrEmpty(isDetailLine))
                {
                    throw new ApplicationException("Invalid member contribution line at the record '" + ++l_intRecordCount + "'");
                }
                else if (!isDetailLine.Equals("D") && !isDetailLine.Equals("H"))
                {
                    throw new ApplicationException("Invalid member contribution line flag at the record '" + ++l_intRecordCount + "'");
                }
                else if (isDetailLine.Equals("D")) // DETAIL RECORDS
                {
                    if (!inputLine.Length.Equals(98)) // Length of detail record (should be 96) //CRF-1634
                    {
                        throw new ApplicationException("Invalid detail line at the record '" + ++l_intRecordCount + "'");
                    }

                    if (!(getZoneCode(empNo) + "" + getEmployeeNumber(empNo)).Equals(getZoneCode(inputLine.Substring(1, 8)) + "" + getEmployeeNumber(inputLine.Substring(1, 8)))) // ETF reg number in screen and detail record
                    {
                        throw new ApplicationException("Employer's ETF registration number at the record '" + ++l_intRecordCount + "' does not match with the screen");
                    }
                    else
                    {
                        d_empRegNumber = inputLine.Substring(1, 8);
                    }

                    try
                    {
                        if (!double.TryParse(inputLine.Substring(89, 9), out amount)) //Read Total Contribution of the member CRF-1634
                        {
                            throw new ApplicationException("Invalid member contribution amount at the record '" + ++l_intRecordCount + "'");
                        }
                        else
                        {

                            d_TotAmount += amount / 100;
                            l_intRecordCount++;
                        }

                    }
                    catch (ArgumentOutOfRangeException ex)
                    {
                        throw new ArgumentOutOfRangeException("Invalid member contribution amount at the record '" + l_intRecordCount + "'");
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Invalid member contribution amount at the record '" + l_intRecordCount + "'");
                    }

                    if (!toFromDates.Equals(inputLine.Substring(77, 12))) // From-To Dates in screen and detail record //CRF-1634
                    {
                        throw new ApplicationException("From-To dates at the record '" + l_intRecordCount + "' does not match with the screen");
                    }
                    else
                    {
                        d_fromToDates = inputLine.Substring(77, 12); //CRF-1634
                    }
                }

                else if (isDetailLine.Equals("H")) // HEADER RECORD
                {
                    if (!inputLine.Length.Equals(43))  // Length of header record (should be 43)
                    {
                        throw new ApplicationException("Header record is invalid");
                    }

                    if (!(getZoneCode(empNo) + "" + getEmployeeNumber(empNo)).Equals(getZoneCode(inputLine.Substring(1, 8)) + "" + getEmployeeNumber(inputLine.Substring(1, 8))))  // ETF reg number in screen and header record
                    {
                        throw new ApplicationException("Employer's ETF number in the header record does not match with the screen");
                    }
                    else
                    {
                        h_empRegNumber = inputLine.Substring(1, 8);
                    }

                    h_totalRecords = int.Parse(inputLine.Substring(21, 6));


                    try
                    {
                        if (!double.TryParse(inputLine.Substring(27, 14), out h_amount)) //Read Total Contribution of the member
                        {
                            throw new ApplicationException("Invalid member total contribution amount at the header record");
                        }
                        else
                        {
                            h_TotAmount = h_amount / 100;
                        }

                    }
                    catch (ArgumentOutOfRangeException ex)
                    {
                        throw new ArgumentOutOfRangeException("Invalid member total contribution amount at the header record");
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Invalid member total contribution amount at the header record");
                    }

                    if (!toFromDates.Equals(inputLine.Substring(9, 12))) // From to dates in screen and header record
                    {
                        throw new ApplicationException("From-To dates in header record do not match with the screen");
                    }
                    else
                    {
                        h_fromToDates = inputLine.Substring(9, 12);
                    }
                }
                else
                {
                    throw new ApplicationException("Invalid ETF file uploaded");
                }
            }


            if (!h_fromToDates.Equals(d_fromToDates)) // From To dates in detail record and header record
            {
                throw new ApplicationException("From-To dates do not match with header record");
            }

            if (h_totalRecords != l_intRecordCount) // Records in header and detail
            {
                throw new ApplicationException("No. of employees in detail records does not match with the header record");
            }

            if (string.Format("{0:0.00}", d_TotAmount) != string.Format("{0:0.00}", h_TotAmount)) // Total amount in header and detail
            {
                throw new ApplicationException("Total contribution does not match with the header and the detail records");
            }

            if (select == 1)
            {
                if (!l_intRecordCount.Equals(int.Parse(noOfEmp))) // record count in detail and screen records
                {
                    throw new ApplicationException("Number of members does not tally between the file and the payment");
                }

                if (!string.Format("{0:0.00}", d_TotAmount).Equals(string.Format("{0:0.00}", Convert.ToDouble(ttlAmount)))) // Amount in detail and screen records
                {
                    throw new ApplicationException("Amount does not tally between the file and the payment");
                }
            }

            FileSummary[0] = l_intRecordCount.ToString();
            FileSummary[1] = Math.Round(d_TotAmount, 2).ToString();           
            return FileSummary;
        }

        static string[] GetEpfFileInformation(Stream objFileStream, string etfNumber, string conYear, string conMonth, string totalAmount, string numOfEmp)
        {
            objFileStream.Seek(0, SeekOrigin.Begin);
            StreamReader sr = new StreamReader(objFileStream);
            string inputLine = "";
            int l_intRecordCount = 0;
            double d_TotAmount = 0.0d;
            string s_zoneCode = "";
            string S_eMPnO = "";
            string s_conYear = "";
            string p_zcodeNecode = "";
            string l_CurrencyType = string.Empty;
            string s_conMonth = string.Empty;

            string[] FileSummary = new string[2];

            while ((inputLine = sr.ReadLine()) != null)
            {
                l_intRecordCount++;

                if (!inputLine.Length.Equals(152))
                {
                    throw new ApplicationException("Record '" + l_intRecordCount + "' is out of format");
                }

                s_zoneCode = inputLine.Substring(129, 1).ToString();
                S_eMPnO = inputLine.Substring(130, 6).ToString();
                p_zcodeNecode = s_zoneCode + " " + S_eMPnO;
                s_conYear = inputLine.Substring(136, 4).ToString();
                s_conMonth = inputLine.Substring(140, 2).ToString();

                if (!s_zoneCode.Equals(getZoneCode(etfNumber)))
                {
                    throw new ApplicationException("Employer number mismatched at the record '" + l_intRecordCount + "'");
                }

                if (!int.Parse(S_eMPnO).Equals(getEmployeeNumber(etfNumber)))
                {
                    throw new ApplicationException("Employer number mismatched at the record '" + l_intRecordCount + "'");
                }

                if (!s_conYear.Equals(conYear))
                {
                    throw new ApplicationException("Contribution year & month mismatched at the record '" + l_intRecordCount + "'");
                }

                if (!s_conMonth.Equals(conMonth))
                {
                    throw new ApplicationException("Contribution year & month mismatched at the record '" + l_intRecordCount + "'");
                }

                double amount = 0.0d;
                if (!double.TryParse(inputLine.Substring(86, 10), out amount)) //Read Total Contribution of the member
                {
                    throw new ApplicationException("Invalid member contribution amount at the record '" + l_intRecordCount + "'");
                }
                else
                {
                    d_TotAmount += amount;
                }
            }

            if (!l_intRecordCount.Equals(int.Parse(numOfEmp)))
            {
                throw new ApplicationException("Number of members does not tally between the file and the payment");
            }

            string xt = d_TotAmount.ToString();

            string ju = string.Format("{0:0.00}", d_TotAmount);
            string ji = string.Format("{0:0.00}", Convert.ToDouble(totalAmount));

            if (!string.Format("{0:0.00}", d_TotAmount).Equals(string.Format("{0:0.00}", Convert.ToDouble(totalAmount))))
            {
                throw new ApplicationException("Amount does not tally between the file and the payment");
            }

            FileSummary[0] = l_intRecordCount.ToString();
            FileSummary[1] = Math.Round(d_TotAmount, 2).ToString();
            return FileSummary;
        }

        static string[] GetEpfPaymentFileInformation(Stream objFileStream, string etreg, string yearMonth, string totalAmount, string numOfEmp)
        {
            objFileStream.Seek(0, SeekOrigin.Begin);
            StreamReader sr = new StreamReader(objFileStream);
            string inputLine = "";
            int l_intRecordCount = 0;
            string[] FileSummary = new string[2];
            double amount = 0.0d;
            int noofemp = 0;

            while ((inputLine = sr.ReadLine()) != null)
            {
                l_intRecordCount++;

                if (!inputLine.Length.Equals(64) && !inputLine.Length.Equals(65))
                {
                    throw new ApplicationException("Record '" + l_intRecordCount + "' is out of format");
                }

                if (!inputLine.Substring(0, 7).Equals(etreg.Trim().Replace(" ","")))
                {
                    throw new ApplicationException("Employer number mismatched at the record '" + l_intRecordCount + "'");
                }

                if (!inputLine.Substring(7, 6).Equals(yearMonth))
                {
                    throw new ApplicationException("Contribution year & month mismatched at the record '" + l_intRecordCount + "'");
                }
               
                if (!double.TryParse(inputLine.Substring(15, 12), out amount)) //Read Total Contribution of the member
                {
                    throw new ApplicationException("Invalid member contribution amount at the record '" + l_intRecordCount + "'");
                }

                if (amount!= double.Parse(totalAmount.Trim()))
                {
                    throw new ApplicationException("Amount mismatched at the record '" + l_intRecordCount + "'");
                }
               
                if (!int.TryParse(inputLine.Substring(27, 5), out noofemp)) //Read Total Contribution of the member
                {
                    throw new ApplicationException("Invalid members at the record '" + l_intRecordCount + "'");
                }

                if (noofemp != int.Parse(numOfEmp.Trim()))
                {
                    throw new ApplicationException("Members mismatched at the record '" + l_intRecordCount + "'");
                }               
            }

            FileSummary[0] = noofemp.ToString();
            FileSummary[1] = Math.Round(amount, 2).ToString();

            return FileSummary;
        }

        static int getEmployeeNumber(string code)
        {

            return int.Parse(Regex.Match(code, @"\d+").Value);
        }

        static string getZoneCode(string code)
        {
            Regex rgx = new Regex("[^a-zA-Z]");
            return rgx.Replace(code, "");
        }

        static byte[] GenerateEPF_EVEMP(etf_epf_record tran)
        {
            //Generate EVEMP.TXT File
            StringBuilder evemp = new StringBuilder();
            evemp.Append(tran.etreg.Trim().Substring(0, 1));//ZnCode(1)

            string initialString = tran.etreg.Trim();
            Regex nonNumericCharacters = new Regex(@"[^0-9]");
            string numericOnlyString = nonNumericCharacters.Replace(initialString, String.Empty);

            evemp.Append(numericOnlyString.Trim().PadLeft(6, Convert.ToChar(" ")));//Employer Number(6)
            evemp.Append(tran.etfrpd);//Contribution Year and Month(6)
            evemp.Append("1".PadLeft(2, Convert.ToChar(" ")));//Data Submission Number(1)          
            evemp.Append(string.Format("{0:0.00}", Convert.ToDouble(tran.etamt.Trim())).Trim().PadLeft(12, Convert.ToChar(" ")));//Total Contribution Amount(12)(11.2)
            evemp.Append(tran.etnomb.Trim().PadLeft(5, Convert.ToChar(" ")));//Number of Members contributed for(5)
            evemp.Append("2");//Mode of Payment(1)[1=Cheque,2=Cash,3=Money Order,4=Direct Debit]
            evemp.Append((tran.etbkcd + tran.etbrcd.Trim().PadLeft(3, Convert.ToChar("0")) + tran.etacct).Trim().PadRight(20, Convert.ToChar(" ")));//Payment Reference(20) [BankCode(4)BranchCode(3)AccountNumber(Rest)]
            evemp.Append(tran.etadat.Trim().PadLeft(10, Convert.ToChar(" ")));//Date of Payment(10)[yyyymmdd]
            evemp.Append(tran.etdcd);//District office Code

            List<byte> list = new List<byte>();

            foreach (char ch in evemp.ToString().ToCharArray())
            {
                list.Add((byte)((int)ch));
            }

            byte[] buffer = list.ToArray();
            return buffer;
        }

        static void GenerateEPFPaymentFiles(string rootpath, string filter)
        {

            List<etf_epf_record> RefListEPF = GetListEPF(filter);
            var path = string.Empty;

            foreach (var tran in RefListEPF)
            {
                byte[] bufferEmc = GenerateEPF_EVEMP(tran);

                path = rootpath + @"\" + tran.etcopr;
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                path = rootpath + @"\" + tran.etcopr + @"\" + tran.etref + "_" + tran.etcopr.Trim() + "_" + tran.ettran.Trim() + "_EVEMP.txt";
                using (FileStream fileStream = System.IO.File.Create(path, (int)bufferEmc.Length))
                {
                    // Use FileStream object to write to the specified file
                    fileStream.Write(bufferEmc, 0, bufferEmc.Length);
                }

            }
        }

        static void RenameEPFContributionFiles(string rootpath, string filter, string contributionFilesAvailablePath)
        {

            List<etf_epf_record> RefListEPF = GetListEPF(filter);

            List<filePath> FileswithPaths = new List<filePath>();
            var allFiles = Directory.GetFiles(contributionFilesAvailablePath, "*.txt", SearchOption.AllDirectories);

            foreach (var file in allFiles)
            {
                FileswithPaths.Add(new filePath { fullPath = file });
            }

            var path = string.Empty;

            foreach (var tran in RefListEPF)
            {

                filePath has = FileswithPaths.Find(file => file.fullPath.Contains(tran.etref));

                if (has != null)
                {
                    if (!string.IsNullOrEmpty(has.fullPath.ToString()))
                    {
                        path = rootpath + @"\" + tran.etcopr;
                        if (!Directory.Exists(path))
                        {
                            Directory.CreateDirectory(path);
                        }

                        path = rootpath + @"\" + tran.etcopr + @"\" + tran.etref + "_" + tran.etcopr.Trim() + "_" + tran.ettran.Trim() + "_EVEMC.txt";
                        File.Copy(has.fullPath.ToString(), path, true);
                    }
                }
            }

        }

        static void RenameEPFPaymentFiles(string rootpath, string filter, string contributionFilesAvailablePath)
        {

            List<etf_epf_record> RefListEPF = GetListEPF(filter);

            List<filePath> FileswithPaths = new List<filePath>();
            var allFiles = Directory.GetFiles(contributionFilesAvailablePath, "*EVEMP.txt", SearchOption.AllDirectories);

            foreach (var file in allFiles)
            {
                FileswithPaths.Add(new filePath { fullPath = file });
            }

            var path = string.Empty;

            foreach (var tran in RefListEPF)
            {

                filePath has = FileswithPaths.Find(file => file.fullPath.Contains(tran.etref));

                if (has != null)
                {
                    if (!string.IsNullOrEmpty(has.fullPath.ToString()))
                    {
                        path = rootpath + @"\" + tran.etcopr;
                        if (!Directory.Exists(path))
                        {
                            Directory.CreateDirectory(path);
                        }

                        path = rootpath + @"\" + tran.etcopr + @"\" + tran.etref + "_" + tran.etcopr.Trim() + "_" + tran.ettran.Trim() + "_EVEMP.txt";
                        File.Copy(has.fullPath.ToString(), path, true);
                    }
                }
            }

        }

        static void RenameETFContributionFiles(string rootpath, string filter, string contributionFilesAvailablePath)
        {

            List<etf_epf_record> RefListETF = GetListETF(filter);

            List<filePath> FileswithPaths = new List<filePath>();
            var allFiles = Directory.GetFiles(contributionFilesAvailablePath, "*.txt", SearchOption.AllDirectories);

            foreach (var file in allFiles)
            {
                FileswithPaths.Add(new filePath { fullPath = file });
            }

            var path = string.Empty;

            foreach (var tran in RefListETF)
            {

                filePath has = FileswithPaths.Find(file => file.fullPath.Contains(tran.etref));

                if (has != null)
                {
                    if (!string.IsNullOrEmpty(has.fullPath.ToString()))
                    {
                        path = rootpath + @"\" + tran.etdate;
                        if (!Directory.Exists(path))
                        {
                            Directory.CreateDirectory(path);
                        }

                        path = rootpath + @"\" + tran.etdate + @"\";

                        int length = has.fullPath.Length;
                        int lastIndex = has.fullPath.LastIndexOf(@"\");
                        string fileName=has.fullPath.Substring(lastIndex + 1, (length -1)- lastIndex);
                        path += fileName;
                        File.Copy(has.fullPath.ToString(), path, true);
                    }
                }
            }

        }
        
        static void CopyandRenameETFContributionFiles(string rootpath, string filter, string contributionFilesAvailablePath)
        {

            List<etf_epf_record> RefListETF = GetListETF(filter);

            List<filePath> FileswithPaths = new List<filePath>();
            var allFiles = Directory.GetFiles(contributionFilesAvailablePath, "*.txt", SearchOption.AllDirectories);

            foreach (var file in allFiles)
            {
                FileswithPaths.Add(new filePath { fullPath = file });
            }

            var path = string.Empty;

            //Sample File name
            //CBCETF96464_A 049940_2017-September_2017-September_CD.txt      
            //CBCETF93107_PF000390_2017-July_2017-July_CD.TXT
            foreach (var tran in RefListETF)
            {

                filePath has = FileswithPaths.Find(file => file.fullPath.Contains(tran.etref));

                if (has != null)
                {
                    if (!string.IsNullOrEmpty(has.fullPath.ToString()))
                    {
                        path = rootpath + @"\" + tran.etdate;
                        if (!Directory.Exists(path))
                        {
                            Directory.CreateDirectory(path);
                        }

                        path = rootpath + @"\" + tran.etdate + @"\";

                        int length = has.fullPath.Length;
                        int lastIndex = has.fullPath.LastIndexOf(@"\");
                        string fileName = has.fullPath.Substring(lastIndex + 1, (length - 1) - lastIndex);
                        path += tran.etref + "_" + tran.etreg + "_" + tran.etfrpd.Trim().Substring(0, 4) + "-" + CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(int.Parse(tran.etfrpd.Trim().Substring(4, 2)))
                                + "_" + tran.ettopd.Trim().Substring(0, 4) + "-" + CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(int.Parse(tran.ettopd.Trim().Substring(4, 2))) + "_" + tran.etptyp + ".TXT";
                                
                                
                        File.Copy(has.fullPath.ToString(), path, true);
                    }
                }
            }

        }

    }

    class etf_epf_record
    {
        public string etref { get; set; }
        public string etreg { get; set; }
        public string etamt { get; set; }
        public string etpflg { get; set; }
        public string ettime { get; set; }
        public string etdate { get; set; }
        public string etptyp { get; set; }
        public int count { get; set; }
        public string files { get; set; }
        public string etfrpd { get; set; }
        public string ettopd { get; set; }
        public string summary { get; set; }
        public string errors { get; set; }
        public string etnomb { get; set; }
        public string etbkcd { get; set; }
        public string etbrcd { get; set; }
        public string etacct { get; set; }
        public string etdcd { get; set; }
        public string etadat { get; set; }
        public string etcopr { get; set; }
        public string ettran { get; set; }
    }

    class filePath
    {
        public string fullPath { get; set; }
    }

}
