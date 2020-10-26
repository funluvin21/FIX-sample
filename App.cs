using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Sockets;
using System.Threading;
using System.IO;
using System.Runtime.InteropServices;
using System.Timers;

namespace FIX_sample
{
    class App
    {
        #region Member Variables

        int _junk_id = 0;

        /////////////////////////////////
        // Fix Stuff
        /////////////////////////////////
        private int _message_seq_num = 0;
        private StreamWriter _fixlog;
        private bool _debug_mode = true;
        private System.Timers.Timer _system_timer; // heartbeat timer

        /////////////////////////////////
        // Sockets & Threads Stuff
        //  (Uses a blocking read)
        /////////////////////////////////

        private Thread _socket_thread = null; // this is the main thread we get message on
        private TcpClient _socket_tcpclient; // we get data using a blocking read on socket_thread
        private NetworkStream _socket_networkstream;

        private byte[] _buffer_data = new byte[655360]; // buffer
        private int _buffer_end = 0; // this is the number of bytes of data in the buffer 
        private int _buffer_start = 0; // this is posn of current message

        #endregion

        #region Constructor, Timer, Error Handler

        public App()
        {
            _fixlog = new StreamWriter("fixlog.txt");
            _socket_tcpclient = new TcpClient();
            _socket_tcpclient.NoDelay = true;
            _buffer_end = 0;
            try
            {
                //_socket_tcpclient.Connect("localhost", 1000);
                _socket_tcpclient.Connect("192.168.209.70", 55000); // Tunnel
                _socket_networkstream = _socket_tcpclient.GetStream();
                _socket_thread = new Thread(new ThreadStart(this.ReadData));
                _socket_thread.Start();
            }
            catch
            {
                Console.WriteLine("Failed to Connect");
                return;
            }
            Console.WriteLine("Connected");
        }

        private void OnTimerEvent(object source, ElapsedEventArgs e)
        {
            // This is shit becuase by running on another thread the message sequence number
            // could get out of order. But it's the way SG & Chris did it so I will take the risk...
            SendHeartBeat();
        }

        private void ErrorHandler(string s, int fix_message_buffer_start, int fix_message_buffer_length)
        {
            _system_timer.Enabled = false;
            _fixlog.WriteLine("Fatal Error");
            _fixlog.WriteLine(s);
            if (fix_message_buffer_length > 0)
                _fixlog.WriteLine(Encoding.ASCII.GetString(_buffer_data, fix_message_buffer_start, fix_message_buffer_length));
            _fixlog.Flush();
            Console.WriteLine("Fatal Error");
            Console.WriteLine(s);
            if (_socket_thread != null) { _socket_thread.Abort(); Console.WriteLine("Thread aborted"); };
            _socket_thread = null;
        }

        #endregion

        #region SocketReadStuff & FixSendMessage

        private void ReadData()
        {
            while (true)
            {
                // we are using a blocking read.. so at this point we will always be exactly at the start of a new fix message
                // read at least enough data to get the message length
                ReadSocket(25);
                // Stre the location of the start of the fix message in fix_message_buffer_start
                int fix_message_buffer_start = _buffer_start;
                // message starts "8=FIXT.1.19=" which has length 13
                if (_buffer_data[_buffer_start] != '8' || _buffer_data[_buffer_start + 1] != '=')
                    ErrorHandler("message did not start with 8=", fix_message_buffer_start, 25);
                _buffer_start += 13;
                // read the message length
                int length = ReadIntValue(ref _buffer_start);
                // Store the total length of the message in fix_message_buffer_length
                int fix_message_buffer_length = _buffer_start - fix_message_buffer_start + length + 7;
                // we are only sure we have read the first 25 bytes of the message - now read the rest
                ReadSocket(fix_message_buffer_length - 25);
                // log this message if debugging on
                if (_debug_mode)
                {
                    if (fix_message_buffer_length < 10000)
                    {
                        _fixlog.WriteLine(Encoding.ASCII.GetString(_buffer_data, fix_message_buffer_start, fix_message_buffer_length));
                        _fixlog.Flush();
                    }
                    else
                    {
                        _fixlog.WriteLine("Message was so long we did not log it!");
                    }
                }
                // now parse the message
                int end = _buffer_start + length - 1; // _buffer_data[end] is the 01 end field terminator just before the checksum starts
                                                      // Mesage must start with 35=
                if (_buffer_data[_buffer_start] != 51 || _buffer_data[++_buffer_start] != 53 || _buffer_data[++_buffer_start] != 61)
                    ErrorHandler("35= Was not at the start of the message", fix_message_buffer_start, fix_message_buffer_length);
                _buffer_start++;
                string fix_message_id = ReadStringValue(ref _buffer_start);
                // Now skip the next four fields which are SenderCompId and MsgSeqNum and TargetCompId and SendingTime
                SkipValue(ref _buffer_start);
                SkipValue(ref _buffer_start);
                SkipValue(ref _buffer_start);
                SkipValue(ref _buffer_start);
                ProcessFixMessage(fix_message_id, fix_message_buffer_start, fix_message_buffer_length, _buffer_start, end);
                // Move to end of message
                _buffer_start = fix_message_buffer_start + fix_message_buffer_length;
                // Clean the buffer out if there are no more messages inside it (normally the case)
                if (_buffer_start == _buffer_end) _buffer_start = _buffer_end = 0;
            }

        }

        private int ReadKey(ref int posn)
        {
            int val = _buffer_data[posn] - 48;
            while (_buffer_data[++posn] != 61) val = val * 10 + _buffer_data[posn] - 48;
            posn++;
            return val;
        }

        private string ReadStringValue(ref int posn)
        { // we assume the value is at least one character long
            string val;
            int posn_temp = posn;
            while (_buffer_data[++posn] != 1) { }
            val = Encoding.ASCII.GetString(_buffer_data, posn_temp, posn - posn_temp);
            posn++;
            return val;
        }

        private int ReadIntValue(ref int posn)
        {
            int val = _buffer_data[posn] - 48;
            while (_buffer_data[++posn] != 1) val = val * 10 + _buffer_data[posn] - 48;
            posn++;
            return val;
        }

        private void SkipValue(ref int posn)
        { // we assume the value is at least one character long
            while (_buffer_data[++posn] != 1) { }
            posn++;
        }

        private void ReadSocket(int numbytes) // Block until we have numbytes of data in the buffer
        {
            int bytes_available = _buffer_end - _buffer_start;
            while (bytes_available < numbytes)
            {
                try
                {
                    int size = _socket_networkstream.Read(_buffer_data, _buffer_end, 655360 - _buffer_end);
                    if (size == 0) ErrorHandler("Socket Dead", 0, 0);
                    _buffer_end += size;
                    // record the time now - this is the time before message processing -
                    //  if the message arrived in parts this is the first time all the data we need is in the buffer
                    if (_debug_mode)
                    {
                        if (_buffer_start > 0)
                            _fixlog.WriteLine(size + " bytes received into middle of buffer");
                        else
                            _fixlog.WriteLine(size + " bytes received into start of buffer");
                    }
                }
                catch
                {
                    ErrorHandler("Socket Dead", 0, 0);
                }
                bytes_available = _buffer_end - _buffer_start;
            }
        }

        private void FixSendMessage(string type, string body)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("35=");
            sb.Append(type);
            sb.Append("49=X56=ICAP_AI_Server");
            sb.Append("34=");
            sb.Append((++_message_seq_num).ToString());
            sb.Append("52=");
            sb.Append(DateTime.UtcNow.ToString("yyyyMMdd-HH:mm:ss"));
            sb.Append('');
            sb.Append(body);
            string t = sb.ToString();
            sb.Length = 0;
            sb.Append("8=FIXT.1.19=");
            sb.Append((t.Length).ToString());
            sb.Append("");
            sb.Append(t);
            t = sb.ToString();
            int sum = 0;
            int len = t.Length;
            for (int i = 0; i < len; i++) sum += Convert.ToChar(t.Substring(i, 1));
            sum = sum % 256;
            sb.Append("10=");
            sb.Append(sum.ToString("000"));
            sb.Append('');
            byte[] buffer = Encoding.ASCII.GetBytes(sb.ToString());
            _socket_networkstream.Write(buffer, 0, buffer.Length);
            if (_debug_mode)
            {
                _fixlog.WriteLine(">>>" + sb.ToString());
                _fixlog.Flush();
            }
        }

        #endregion

        #region ProcessFixMessage

        private void ProcessFixMessage(string message_id, int message_buffer_start, int message_buffer_length, int start, int end)
        {
            //////////////////////////////////////////////////////////////
            // Message types:
            // "A" FixLogonResponseMsg		
            // "5" FixLogonFailedResponseMsg
            // "BF" LogonResponseMsg
            // "0" Heartbeat
            // "1" HeartbeatRequestMsg
            // "8" OrderEventMsg
            // "9" OrderInterruptFailedMsg
            // "W" MarketSnapshotEventMsg
            // "X" MarketUpdateEventMsg
            // "Y" MarketRejectSubscriptionEventMsg
            // "h" SessionEventMsg
            // "AE" DealEventMsg
            //////////////////////////////////////////////////////////////

            if (message_id == "X") // MarketUpdateEventMsg
            {
                // _fix_message_key[0] = 262 MDReqID
                SkipValue(ref start);
                // _fix_message_key[1] = 268 NoMDEntries - a one or two character integer
                if (ReadKey(ref start) != 268) ErrorHandler("NoMDEntries not found in expected place", message_buffer_start, message_buffer_length);
                int kNoMDEntries = ReadIntValue(ref start);
                int key = -1;
                for (int j = 0; j < kNoMDEntries; j++)
                {
                    // 279 MDUpdateAction
                    if (key < 0) key = ReadKey(ref start);
                    while (key != 279)
                    {
                        SkipValue(ref start);
                        key = ReadKey(ref start);
                    }
                    key = -1;
                    SkipValue(ref start);

                    // 269 MDEntryType ( 0 = bid, 1 =offer, 2= trade )
                    if (ReadKey(ref start) != 269) ErrorHandler("MDEntryType not found in expected place", message_buffer_start, message_buffer_length);
                    int kMDEntryType = ReadIntValue(ref start);
                    if (kMDEntryType != 2)
                    {
                        if (ReadKey(ref start) != 5450) ErrorHandler("MDElementName not found in expected place", message_buffer_start, message_buffer_length);
                        int kMDElementName = ReadIntValue(ref start);
                        // 5450 MDElementName - a one or two character integer
                        // 1	best_bid
                        // 2	best_offer
                        // 11	paid
                        // 12	given
                        // 45	dealable_bid
                        // 46	dealable_offer
                        // 47	local_bid
                        // 48	local_offer
                        // 49	dealable_regular_bid
                        // 50	dealable_regular_offer
                        // 51	dealable_outside_bid
                        // 52	dealable_outside_offer
                        // 53	dealable_plus_bid
                        // 54	dealable_plus_offer 
                        if (kMDElementName == 45 || kMDElementName == 46)
                        {
                            // 55 Symbol
                            if (ReadKey(ref start) != 55) ErrorHandler("Symbol not found in expected place", message_buffer_start, message_buffer_length);
                            string kSymbol = ReadStringValue(ref start);
                            // 461 CFICode = RSCXXX
                            if (ReadKey(ref start) != 461) ErrorHandler("CFICode not found in expected place", message_buffer_start, message_buffer_length);
                            SkipValue(ref start);
                            // 63 SettlType = 0
                            if (ReadKey(ref start) != 63) ErrorHandler("SettlType not found in expected place", message_buffer_start, message_buffer_length);
                            SkipValue(ref start);
                            // Now we have some condition keys...
                            key = ReadKey(ref start);
                            // 276 QuoteCondition - Conditional 
                            if (key == 276)
                            {
                                SkipValue(ref start);
                                key = ReadKey(ref start);
                            }
                            // 270 MDEntryPx - Conditional
                            string price = "0";
                            if (key == 270)
                            {
                                price = ReadStringValue(ref start);
                                key = ReadKey(ref start);
                            }
                            // 277 TradeCondition - Conditional ... Ignore if it exsits 
                            if (key == 277)
                            {
                                SkipValue(ref start);
                                key = ReadKey(ref start);
                            }
                            // 271 MdEntrySize - Conditional
                            int size = 0;
                            if (key == 271)
                            {
                                size = ReadIntValue(ref start) / 1000000;
                                key = ReadKey(ref start);
                            }
                            if (_debug_mode)
                            {
                                if (kMDElementName == 45)
                                    _fixlog.WriteLine("Dealable Bid   " + kSymbol + " " + price + " " + size);
                                else if (kMDElementName == 46)
                                    _fixlog.WriteLine("Dealable Offer " + kSymbol + " " + price + " " + size);
                            }
                            if (kSymbol == "EUR/USD" && _junk_id == 0 && kMDElementName == 45)
                            {
                                Console.WriteLine("Send Order");
                                _fixlog.WriteLine("Buy " + kSymbol + " " + price + " " + size);
                                _junk_id++;
                                SendOrder(_junk_id.ToString(), kSymbol, price, "4", SendOrderType.BID);
                            }
                        }
                    }
                }
                return;
            }

            if (message_id == "8") // OrderEventMsg
            {
                Console.WriteLine("OrderEventMsg");
                // This message does not come out in a consistent order so we loop round with select to process
                string sClOrdID = null; // 11
                string sSymbol = null; // 55
                string sExecType = null; // 150
                                         // 0 = Accepted
                                         // 4 = Cancelled
                                         // 8 = Rejected
                                         // C = Expired
                                         // F = Trade (partial or full fill)
                int iLeavesQty = 0; // 151 (auantity remaining)
                int iCumQty = 0; // 14 (total quantility filled)
                string sText = null; // 58 (reject reason)

                while (start < end)
                {
                    int key = ReadKey(ref start);
                    switch (key)
                    {
                        case 11:
                            sClOrdID = ReadStringValue(ref start);
                            break;
                        case 55:
                            sSymbol = ReadStringValue(ref start);
                            break;
                        case 150:
                            sExecType = ReadStringValue(ref start);
                            break;
                        case 151:
                            iLeavesQty = ReadIntValue(ref start) / 1000000;
                            break;
                        case 14:
                            iCumQty = ReadIntValue(ref start) / 1000000;
                            break;
                        case 58:
                            sText = ReadStringValue(ref start);
                            break;
                        default:
                            SkipValue(ref start);
                            break;
                    }
                }

                _fixlog.WriteLine("ORDER_MESSAGE: ExecType=" + sExecType + "," +
                                  "sClOrdID=" + sClOrdID + "," +
                                  sSymbol + "," +
                                  "CumQty=" + iCumQty + "," +
                                  "LeavesQty=" + iLeavesQty + "," +
                                  sText);

                return;
            }

            if (message_id == "9")
            {
                Console.WriteLine("Order Interrupt failed");

            }


            if (message_id == "AE") // Trade Report or Deal mesage
            {
                Console.WriteLine("Trade Report");
                string sOrderId = null; // 37 This is our order id
                string sMatchStatus = null; // 573 - "Z" for Pending, "2" for Confirmed, "0" for Done
                string sSymbol = null; // 55
                int iLastQty = 0; // 32
                string sLastPx = null; // 31
                string sSide = null; // 54

                while (start < end)
                {
                    int key = ReadKey(ref start);
                    switch (key)
                    {
                        case 573:
                            sMatchStatus = ReadStringValue(ref start);
                            break;
                        case 55:
                            sSymbol = ReadStringValue(ref start);
                            break;
                        case 32:
                            iLastQty = ReadIntValue(ref start) / 1000000;
                            break;
                        case 31:
                            sLastPx = ReadStringValue(ref start);
                            break;
                        case 54:
                            sSide = ReadStringValue(ref start);
                            break;
                        case 37:
                            sOrderId = ReadStringValue(ref start);
                            break;
                        default:
                            SkipValue(ref start);
                            break;
                    }
                }

                _fixlog.WriteLine("TRADE_MESSAGE: MatchStatus=" + sMatchStatus + "," +
                                  "Order ID=" + sOrderId + "," +
                                  "Side=" + sSide + "," +
                                    sSymbol + "," + sLastPx + "," + iLastQty);

                return;
            }


            if (message_id == "0") return; // heatbeat

            if (message_id == "A") // FixLogonResponseMsg
            {
                Console.WriteLine("Fix Logon OK");
                SendLogon2();
                return;
            }

            if (message_id == "5") // FixLogonFailedResponseMsg
            {
                ErrorHandler("FixLogonFailedResponseMsg", message_buffer_start, message_buffer_length);
                return;
            }

            if (message_id == "BF") // LogonResponseMsg
            {
                ReadKey(ref start);
                if (ReadIntValue(ref start) == 0)
                { // we did not get a valid TradingSessionID
                    ErrorHandler("Application Logon Failed", message_buffer_start, message_buffer_length);
                    return;
                }
                else
                {
                    Console.WriteLine("Application Logon OK");
                    SendMarketDataRequest();
                    return;
                }
            }

            if (message_id == "W") // MarketSnapshotEventMsg
            {
                // we do not handle the snaphot message
                return;
            }

            if (message_id == "1") // HeartbeatRequestMsg
            {
                Console.WriteLine("Oh dear I did not handle the heartbeat request message yet");
                return;
            }
        }

        #endregion

        #region Send Messages

        private void SendLogon1()
        {
            // 8=FIXT.1.19=8435=A49=test-session056=ICAP_AI_Server34=152=20090402-12:57:2998=0108=11137=710=153
            FixSendMessage("A", "98=0108=11137=7");
        }

        private void SendLogon2()
        {
            //8=FIXT.1.19=11135=BE49=test-session056=ICAP_AI_Server34=252=20090402-12:57:301129=1.0923=XCT924=1553=XCT554=chris12310=159
            FixSendMessage("BE", "1129=1.0923=XCT924=1553=XCT554=chris123");
        }

        private void SendHeartBeat()
        {
            //8=FIXT.1.19=6635=049=test-session056=ICAP_AI_Server34=452=20090402-12:57:3110=092
            try
            {
                FixSendMessage("0", null);
                // lets do some trading without subscribing to the market data...
                //if (++_junk_id == 10)
                //{
                //    Console.WriteLine("Send Order");
                //    SendOrder(_junk_id.ToString(), "EUR/USD", "1.3256", "4", SendOrderType.BID);
                //}
            }
            catch { }
        }

        private void SendMarketDataRequest()
        {
            //8=FIXT.1.19=44235=V49=test-session056=ICAP_AI_Server34=352=20090402-12:57:31
            //262=0263=1264=0265=1267=1269=*146=12
            //55=EUR/USD461=RCSXXX63=0       
            //55=USD/JPY461=RCSXXX63=0
            //55=EUR/JPY461=RCSXXX63=0
            //55=USD/CHF461=RCSXXX63=0
            //55=EUR/CHF461=RCSXXX63=0
            //55=GBP/USD461=RCSXXX63=0
            //55=EUR/GBP461=RCSXXX63=0
            //55=USD/CAD461=RCSXXX63=0
            //55=AUD/USD461=RCSXXX63=0
            //55=NZD/USD461=RCSXXX63=0
            //55=EUR/NOK461=RCSXXX63=0
            //55=EUR/SEK461=RCSXXX63=0
            //10=215
            StringBuilder sb = new StringBuilder();
            sb.Append("262=0263=1264=0265=1267=1269=*146=12");  
            sb.Append("55=EUR/USD461=RCSXXX63=0");
            sb.Append("55=USD/JPY461=RCSXXX63=0");
            sb.Append("55=EUR/JPY461=RCSXXX63=0");
            sb.Append("55=USD/CHF461=RCSXXX63=0");
            sb.Append("55=EUR/CHF461=RCSXXX63=0");
            sb.Append("55=GBP/USD461=RCSXXX63=0");
            sb.Append("55=EUR/GBP461=RCSXXX63=0");
            sb.Append("55=USD/CAD461=RCSXXX63=0");
            sb.Append("55=AUD/USD461=RCSXXX63=0");
            sb.Append("55=NZD/USD461=RCSXXX63=0");
            sb.Append("55=EUR/NOK461=RCSXXX63=0");
            sb.Append("55=EUR/SEK461=RCSXXX63=0");
            FixSendMessage("V", sb.ToString());
        }

        public enum SendOrderType
        {
            BUY = 0,
            SELL = 1,
            BID = 2,
            OFFER = 3
        }

        private void SendOrder(string id, string ccyccy, string price, string amount, SendOrderType type)
        { // type = "BUY", "SELL", "BID", "OFFER"
            StringBuilder sb = new StringBuilder();
            sb.Append("11=");
            sb.Append(id);
            sb.Append("55=");
            sb.Append(ccyccy);
            sb.Append("461=RCSXXX63=0");
            switch (type)
            {

                case SendOrderType.BUY:
                    sb.Append("54=140=259=344=");
                    break;
                case SendOrderType.SELL:
                    sb.Append("54=240=259=344=");
                    break;
                case SendOrderType.BID:
                    sb.Append("54=140=259=144=");
                    break;
                case SendOrderType.OFFER:
                    sb.Append("54=240=259=144=");
                    break;
            }
            sb.Append(price);
            sb.Append("38=");
            sb.Append(amount);
            sb.Append("00000060=0");
            FixSendMessage("D", sb.ToString());
            _fixlog.WriteLine("SUBMIT_MESSAGE: Id=" + id + "," + type.ToString() + "," + ccyccy + "," + price + "," + amount);

        }

        private void SendOrderCancel(string id, string ccyccy, SendOrderType type)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("11=");
            sb.Append(id);
            sb.Append("41=");
            sb.Append(id);
            sb.Append("55=");
            sb.Append(ccyccy);
            sb.Append("461=RCSXXX63=0");
            switch (type)
            {
                case SendOrderType.BUY:
                    sb.Append("54=140=259=360=038=0");

                    break;
                case SendOrderType.SELL:
                    sb.Append("54=240=259=360=038=0");
                    break;
                case SendOrderType.BID:
                    sb.Append("54=140=259=160=038=0");
                    break;
                case SendOrderType.OFFER:
                    sb.Append("54=240=259=160=038=0");
                    break;
            }
            FixSendMessage("F", sb.ToString());
        }

        private void SendAllOrderCancel()
        {
            FixSendMessage("q", "11=99999960=0530=7");
        }

        #endregion
    }
}
