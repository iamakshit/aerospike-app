/**
 *  Copyright 2015 Jasper Infotech (P) Limited . All Rights Reserved.
 *  JASPER INFOTECH PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.akshit;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

/**
 * @version 1.0, 11-Nov-2015
 * @author akshit
 */
public class TweetService {

    public AerospikeClient client;

    public TweetService(AerospikeClient client) {
        this.client = client;
    }

    public void createTweet(Scanner ak) {
        System.out.println("Enter the username");
        String username = ak.next();
        System.out.println("You entered the userName : " + username);

        Key userKey = new Key("test", "users", username);
        Record userRecord = client.get(null, userKey);
        Integer nextTweetCount = (Integer) userRecord.getValue("tweetcount") + 1;

        System.out.println("Enter the tweet");
        String tweet = ak.next();

        WritePolicy wPolicy = new WritePolicy();
        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

        Key tweetKey = new Key("test", "tweets", username + ":" + nextTweetCount);
        Bin bin1 = new Bin("tweet", tweet);
        Bin bin2 = new Bin("ts", new Timestamp(new Date().getTime()));
        Bin bin3 = new Bin("username", username);

        client.put(wPolicy, tweetKey, bin1, bin2, bin3);
        System.out.println("\nINFO: Tweet record created!\n");

        Bin bin4 = new Bin("tweetcount", nextTweetCount);
        client.put(wPolicy, userKey, bin4);

        //Policy is null be default

    }

    public void scanAllTweetsForAllUsers() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.includeBinData = true;
        scanPolicy.priority = Priority.LOW;
        client.scanAll(scanPolicy, "test", "tweets", new ScanCallback() {

            public void scanCallback(Key key, Record record) throws AerospikeException {
                // TODO Auto-generated method stub
                System.out.print(record.getValue("username") + " ");
                System.out.println(record.getValue("tweet"));
            }
        }, "username", "tweet");
    }

    public void queryTweetsByUsername(String username) throws AerospikeException{
        RecordSet recordSet = null;

        String[] bins = { "tweet" };

        Statement statement = new Statement();
        statement.setBinNames(bins);

        Filter filter = Filter.equal("username", username);
        statement.setFilters(filter);

        //        statement.setIndexName(indexName);        We are supposed to use this only if we create indexes here which we don't

        statement.setNamespace("test");
        statement.setSetName("tweets");
        recordSet = client.query(null, statement);

        while (recordSet.next()) {
            Record record = recordSet.getRecord();
            System.out.println("==>" + record);
        }

        recordSet.close();
    }

    public void queryUsersByTweetCount(Integer min, Integer max) throws AerospikeException {
        RecordSet recordSet = null;
        Statement statement = new Statement();
        String[] bins = { "username" };
        Filter filter = Filter.range("tweetCount", min, max);
        statement.setFilters(filter);

        //        statement.setIndexName(indexName);        We are supposed to use this only if we create indexes here which we don't

        statement.setNamespace("test");
        statement.setSetName("users");
        recordSet = client.query(null, statement);

        while (recordSet.next()) {
            Record record = recordSet.getRecord();
            System.out.println("==>" + record.getValue("username"));
        }

        recordSet.close();
    }
}
