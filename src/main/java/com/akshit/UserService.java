/**
 *  Copyright 2015 Jasper Infotech (P) Limited . All Rights Reserved.
 *  JASPER INFOTECH PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.akshit;

import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

/**
 * @version 1.0, 09-Nov-2015
 * @author akshit
 */
public class UserService {

    private AerospikeClient client;

    public UserService(AerospikeClient client) {
        this.client = client;
    }

    public void createUser(Scanner ak) {
        System.out.println("\n********** Create User **********\n");

        String username;
        String password = "snap@123";
        String gender = "M";
        String region = "Delhi";
        String interests = "Cricket";

        System.out.println("Enter userName .. ");
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        username = ak.next();
        Key key = new Key("test", "users", username);

        Bin bin1 = new Bin("username", username);
        Bin bin2 = new Bin("password", password);
        Bin bin3 = new Bin("gender", gender);
        Bin bin4 = new Bin("region", region);
        Bin bin5 = new Bin("interests", interests);
        Bin bin6 = new Bin("lasttweeted", 0);
        Bin bin7 = new Bin("tweetcount", 0);

        client.put(writePolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

        System.out.println("Sucessfully updated");

    }

    public void getUser(Scanner ak) {
        System.out.println("Enter the username");
        String username = ak.nextLine();

        Key userKey = new Key("test", "users", username);
        Record userRecord = client.get(null, userKey);
        if (userRecord != null) {
            System.out.println("UserName :" + userRecord.getValue("username") + "  password :" + userRecord.getValue("password") + "  gender :" + userRecord.getValue("gender")
                    + "  region :" + userRecord.getValue("region") + "  interests :" + userRecord.getValue("interests") + "  tweetcount :" + userRecord.getValue("tweetcount"));

        } else {
            System.out.println("No record for the userName " + username);
        }

    }

    public void batchGetUserTweets(Scanner ak) {
        String username = ak.next();

        Key userKey = new Key("test", "users", username);
        Record userRecord = client.get(null, userKey);

        Integer tweetCount = (Integer) userRecord.getValue("tweetcount");
        System.out.println("TweetCount:" + tweetCount);

        Key[] keysList = new Key[tweetCount];
        for (int i = 1; i <= tweetCount; i++) {
            String key = username + ":" + i;
            Key tweetKey = new Key("test", "tweets", key);
            keysList[i - 1] = tweetKey;
        }

        Record[] records = client.get(null, keysList);
        for (Record record : records) {
            System.out.println("username: " + record.getValue("username") + " tweet:" + record.getValue("tweet"));
        }
    }
}
