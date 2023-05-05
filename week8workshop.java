import java.io.*;
import java.net.*;

public class dsClient {
    public static void main(String[] args) {

        try {
            // create a socket
            Socket s = new Socket("localhost", 50000);
            // Initialise input and output streams associated with the socket
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));

            // connect ds-server
            // Send HELO
            out.write(("HELO\n").getBytes());
            out.flush();
            // System.out.println("sent HELO");
            String str = (String) in.readLine();
            // System.out.println("RCVD: " + str);

            // Send AUTH username
            String username = System.getProperty("user.name");
            out.write(("AUTH " + username + "\n").getBytes());
            out.flush();
            // System.out.println("sent AUTH");
            str = (String) in.readLine();
            // System.out.println("RCVD: " + str);

            String[] serverInfo;
            String serverType = "";
            String serverID = "";

            // While the last message from ds-server is not NONE do
            while (true) {
                out.write(("REDY\n").getBytes());
                out.flush();
                // System.out.println("sent REDY");
                str = in.readLine();
                // System.out.println("RCVD: " + str); // one of the following type: JOBN,JCPL
                // orNone

                if (str.equals("NONE"))
                    break;

                // store the type of job and its id
                String[] jobInfo = str.split(" ");
                String jobType = jobInfo[0];
                String jobID = jobInfo[2];

                if (jobType.equals("JCPL"))
                    continue;

                String jobCore = jobInfo[4];
                String jobRam = jobInfo[5];
                String jobDisk = jobInfo[6];

                out.write(("GETS Capable" + " " + jobCore + " " + jobRam + " " + jobDisk + "\n").getBytes());
                out.flush();
                // System.out.println("sent GETS Capable");
                str = (String) in.readLine(); // DATA X Y
                // System.out.println("RCVD: " + str);

                out.write(("OK\n").getBytes());
                out.flush();
                // System.out.println("sent OK");

                String[] serverInfoList = str.split(" ");
                int nRecs = Integer.parseInt(serverInfoList[1]); // get the number of servers

                // get the first server in the list and schedule the job to it
                str = (String) in.readLine();
                // System.out.println("RCVD: " + str);
                serverInfo = str.split(" ");
                serverType = serverInfo[0];
                serverID = serverInfo[1];

                for (int i = 1; i < nRecs; i++) {
                    str = (String) in.readLine();
                    // System.out.println("RCVD: " + str);
                }
                out.write(("OK\n").getBytes());
                out.flush();
                // System.out.println("sent OK");
                str = (String) in.readLine(); // .
                // System.out.println("RCVD: " + str);

                // schedule the job if it's JOBN
                if (jobType.equals("JOBN")) {
                    String schdMsg = "SCHD " + jobID + " " + serverType + " " + serverID + "\n";
                    out.write(schdMsg.getBytes());
                    out.flush();
                    str = (String) in.readLine();
                    // System.out.println("RCVD: " + str);
                }
            }

            out.write(("QUIT\n").getBytes());
            out.flush();
            // System.out.println("sent QUIT");
            str = in.readLine();
            // System.out.println("RCVD: " + str);
            in.close();
            out.close();
            s.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
