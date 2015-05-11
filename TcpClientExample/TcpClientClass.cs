using System;
using System.Text;
using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Collections.Generic;

namespace TcpClientExample
{
    class TcpClientClass
    {
        static void Main(string[] args)
        {
            /*
             * example 1 - ClickstreamDataScheduler.createQueueEntry
             * command = ClickstreamDataScheduler.createQueueEntry
             * arguments = --b5395118-887b-42de-805f-9e0bd9cbc74d, DATA_FEED, --file:{some_place}, "", READY_WAITING_FOR_CLEANUP

            string command = "ClickstreamDataScheduler.createQueueEntry";
            List<string> arguments = new List<string>();
                arguments.Add("b5395118-887b-42de-805f-9e0bd9cbc222");
                arguments.Add("DATA_FEED");
                arguments.Add("--file:{some_place}");
                arguments.Add("");
                arguments.Add("READY_WAITING_FOR_CLEANUP");
            */

            /*
             * example 2 - DatabaseConnector.getWork
             * command = DatabaseConnector.getWork
             * arguments = --DL_PROCESSOR|0
            */
            string command = "DatabaseConnector.getWork";
            List<string> arguments = new List<string>();
                arguments.Add("DL_PROCESSOR");
                arguments.Add("0");
            
            Console.WriteLine(TcpClientClass.sendServerRequest(command, arguments));
            System.Threading.Thread.Sleep(10000);
        }

        public static string sendServerRequest(string command, List<string> arguments)
        {
            try
            {
                TcpClient client = new TcpClient();
                IPEndPoint serverEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 3000);

                client.Connect(serverEndPoint);
                NetworkStream clientStream = client.GetStream();

                ASCIIEncoding encoder = new ASCIIEncoding();
                byte[] buffer = encoder.GetBytes(command + "--" + string.Join("|", arguments.ToArray()));

                clientStream.Write(buffer, 0, buffer.Length);
                clientStream.Flush();

                return TcpClientClass.receiveServerRequest(client);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return "";
            }
        }

        private static string receiveServerRequest(object client)
        {
            TcpClient tcpClient = (TcpClient)client;
            NetworkStream clientStream = tcpClient.GetStream();

            byte[] message = new byte[4096];
            int bytesRead;

            while (true)
            {
                bytesRead = 0;

                try
                {
                    bytesRead = clientStream.Read(message, 0, 4096);
                }
                catch
                {
                    break;
                }

                if (bytesRead == 0)
                {
                    break;
                }

                ASCIIEncoding encoder = new ASCIIEncoding();
                string cmd = encoder.GetString(message, 0, bytesRead);

                return cmd;
            }

            tcpClient.Close();
            return "";
        }
    }
}
