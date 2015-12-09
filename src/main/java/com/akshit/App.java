package com.akshit;

import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;

/**
 * @author akshit
 */
public class App {
    public static int    TIMEOUT = 500;
    public static String IP      = "127.0.0.1";

    public static void main(String[] args) {
        AerospikeClient client;
        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = TIMEOUT;
        client = new AerospikeClient(cPolicy, IP, 3000);
        Scanner ak = new Scanner(System.in);
        try {
            System.out.println("INFO: Connecting to Aerospike cluster...");

            // Establishing connection to Aerospike server

            if (client == null || !client.isConnected()) {
                System.out.println("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!");
            } else {
                System.out.println("\nINFO: Connection to Aerospike cluster succeeded!\n");

                // Create instance of UserService
                UserService us = new UserService(client);
                // Create instance of TweetService
                TweetService ts = new TweetService(client);
                // Create instance of UtilityService
                // UtilityService util = new UtilityService(client);

                // Present options
                System.out.println("\nWhat would you like to do:\n");
                System.out.println("1> Create A User \n");
                System.out.println("2> Read A User Record\n");
                System.out.println("3> Batch Read Tweets For A User\n");
                System.out.println("4> Scan All Tweets For All Users\n");
                System.out.println("5> Record UDF -- Update User Password\n");
                System.out.println("6> Query Tweets By Username And Users By Tweet Count Range\n");
                System.out.println("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
                System.out.println("8> Create a Tweet\n");
                System.out.println("0> Exit\n");
                System.out.println("\nSelect 0-7 and hit enter:\n");
                int feature = Integer.parseInt(ak.nextLine());

                if (feature != 0) {
                    switch (feature) {
                        case 1:
                            System.out.println("\n********** Your Selection: Create User And A Tweet **********\n");
                            us.createUser(ak);
                            break;
                        case 2:
                            System.out.println("\n********** Your Selection: Read A User Record **********\n");
                            us.getUser(ak);
                            break;
                        case 3:
                            System.out.println("\n********** Your Selection: Batch Read Tweets For A User **********\n");
                            us.batchGetUserTweets(ak);
                            break;
                        case 4:
                            System.out.println("\n********** Your Selection: Scan All Tweets For All Users **********\n");
                            ts.scanAllTweetsForAllUsers();
                            break;
                        case 5:
                            System.out.println("\n********** Your Selection: Record UDF -- Update User Password **********\n");
                            // us.updatePasswordUsingUDF();
                            break;
                        case 6:
                            System.out.println("\n********** Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n");
                            System.out.println("Enter the username");
                            String username = ak.nextLine();
                            ts.queryTweetsByUsername(username);
                            System.out.println("Fetching users by tweet count");
                            ts.queryUsersByTweetCount(0, 2);

                            break;
                        case 7:
                            System.out.println("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
                            // us.aggregateUsersByTweetCountByRegion();
                            break;
                        case 12:
                            System.out.println("\n********** Create Users **********\n");
                            // us.createUsers();
                            break;
                        case 23:
                            System.out.println("\n********** Create Tweets **********\n");
                            // ts.createTweets();
                            break;
                        case 8:
                            ts.createTweet(ak);
                            break;
                        default:
                            break;
                    }
                }
            }
        } catch (AerospikeException e) {
            System.out.println("AerospikeException - Message: " + e.getStackTrace() + "\n");

        } catch (Exception e) {
            System.out.println("Exception - Message: " + e + "\n");
            e.printStackTrace();

        } finally {
            if (client != null && client.isConnected()) {
                // Close Aerospike server connection
                client.close();

            }
            System.out.println("\n\nINFO: Press any key to exit...\n");
        }

        if (client != null) {
            client.close();
        }
        System.out.println("Hello World!");
    }

}
