package au.edu.rmit;

import au.edu.rmit.entity.PedestrianData;

import java.io.File;
import java.util.List;
import java.util.Scanner;

/**
 * main service
 * java au.edu.rmit.dbserver [heap.*]
 */
public class dbserver {

    private static String filePath;

    public static void main(String args[]) {

        if (args.length != 1) {
            System.err.println("miss argument: heap file");
            return;
        }

        // find heap file
        filePath = args[0];
        File heapFile = new File(filePath);
        if (!heapFile.exists()) {
            System.err.println("heap file not exists");
            return;
        }

        Scanner scan = new Scanner(System.in);
        System.out.println("please input your command as below:");
        System.out.println("1. index [add|drop] [ID|Date_Time|Year|Month|Mdate|Day|Time|Hourly_Counts|Sensor_Id|Sensor_Name|SDT_NAME]");
        System.out.println("2. query [property=value]");
        System.out.println("3. exit");
        while (true) {
            System.out.print(">");
            String str = scan.nextLine();
            str = str.trim();
            if (str.startsWith("index")) {
                index.main(filePath, str.replace("index", "").trim().split("\\s"));
            } else if (str.startsWith("query")) {
                long start = System.currentTimeMillis();
                List<PedestrianData> result = dbquery.query(filePath, str.replace("query", "").trim().split("\\s"));
                long end = System.currentTimeMillis();
                for (PedestrianData pedestrianData : result) {
                    System.out.println(pedestrianData);
                }
                System.out.println("fetched " + result.size() + " rows, elapsed " + (end - start) + "ms.");

            } else if (str.equals("exit")) {
                System.out.println("server shutdown");
                scan.close();
                break;
            } else if (str.equals("")) {
                // do nothing
            } else {
                System.err.println("invalid command");
            }
        }
    }
}
