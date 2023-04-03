import java.io.*;
import java.net.*;

public class DSclient {
	public static void main(String[] args) {

		try {
			// create a socket
			Socket s = new Socket("localhost", 50000);
			// Initialise input and output streams associated with the socket
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));

			// System.out.println("Target IP: " + s.getInetAddress() + "Target Port: " +
			// s.getPort());
			// System.out.println("Local IP: " + s.getLocalAddress() + "Local Port: " +
			// s.getLocalPort());

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

			// create variables for seaching largest server type and id
			int largestCore = 0; 
			int count = 0;
			String largestSerType = "";
			Boolean flag = true;
			int send = 0;
			
		
			while (true) {
				out.write(("REDY\n").getBytes());
				out.flush();
				// System.out.println("sent REDY");
				str = in.readLine();
				// System.out.println("RCVD: " + str); // job message:TYPE submitTime jobID estRuntime core...
				
				//If the jobeType is NONE, indicating no more jobs, so quit the while loop
				if (str.equals("NONE"))
					break;

				// store the type of job and its id
				String[] jobInfo = str.split(" ");
				String jobType = jobInfo[0]; 
				String jobID = jobInfo[2];
				// System.out.println("jobType: " + jobType);
				// System.out.println("jobID: " + jobID);

				// if JCPL continue;
				if (jobType.equals("JCPL"))
					continue; 

				if (flag) { // using flag(true) do it only once
					out.write(("GETS All\n").getBytes());
					out.flush();
					// System.out.println("sent GETS All");
					str = (String) in.readLine();
					// System.out.println("RCVD: " + str); // DATA X Y

					out.write(("OK\n").getBytes());
					out.flush();
					// System.out.println("sent OK");

					String[] serverInfoList = str.split(" ");
					int nRecs = Integer.parseInt(serverInfoList[1]); // get the number of servers
					// System.out.println("nRecs: " + nRecs);

					for (int i = 0; i < nRecs; i++) {
						str = (String) in.readLine();
						// System.out.println("RCVD: " + str);

						// find the largest server type and id
						String[] serverInfo = str.split(" ");
						String serverType = serverInfo[0]; // type
						int coreNum = Integer.parseInt(serverInfo[4]); // core number

						if (coreNum > largestCore) { 
							largestSerType = serverType;
							// largestSerID = serverID;
							largestCore = coreNum;
							count = 1;
						} else if (serverType.equals(largestSerType)) {
							count++;
						}

					}

					out.write(("OK\n").getBytes());
					out.flush();
					// System.out.println("sent OK");
					str = (String) in.readLine();
					// System.out.println("RCVD: " + str); // Received . indicating no more server info
				

				}
				flag = false; //set the flag to false

				// System.out.println("Largest Server Type: " + largestSerType);
				// System.out.println("Largest Server CoreNumber: " + largestCore);

				// schedule the job if it's JOBN
				if (jobType.equals("JOBN")) {
					String schdMsg = "SCHD " + jobID + " " + largestSerType + " " + send + "\n";
					out.write(schdMsg.getBytes());
					out.flush();
					send++; //calculates the number of largest servers
					send = send % count; //using the server from smallest id to largest id, then start from smallest again.
					// System.out.println("sent SCHD: " + schdMsg);
					str = in.readLine();
					// System.out.println("RCVD: " + str);
				}
			}
		
			//quit simulation gracefully 
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

